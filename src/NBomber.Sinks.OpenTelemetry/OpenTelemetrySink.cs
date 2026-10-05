using System.Collections.Concurrent;
using Microsoft.Extensions.Configuration;
using NBomber.Contracts;
using NBomber.Contracts.Metrics;
using NBomber.Contracts.Stats;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using Serilog;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace NBomber.Sinks.OpenTelemetry;

/// <summary>
/// Reporting sink for NBomber that exports performance metrics and scenario statistics
/// to OpenTelemetry-compatible systems using the OTLP protocol (e.g., Prometheus, New Relic, Dynatrace, Datadog).
/// </summary>
public class OpenTelemetrySink : IReportingSink
{
    private ILogger? _logger;
    private IBaseContext? _context;
    private MeterProvider? _meterProvider;
    private Meter? _meter;
    private EmptyMetricsReader? _customMetricsReader;
    private OtlpExporterOptions? _config;
    private readonly MetricReaderTemporalityPreference _readerTemporalityPreference;
    private OpenTelemetrySdkEventListener? _eventListener;

    // Tags are stable for the whole test session, so they are built once and then reused.
    private OperationType? _cachedOperationType;
    private Dictionary<string, object?>? _globalTags;
    private readonly ConcurrentDictionary<string, KeyValuePair<string, object?>[]> _scenarioTags = new();
    private readonly ConcurrentDictionary<(string Scenario, string Step), TagList> _stepTags = new();
    private readonly ConcurrentDictionary<(string Scenario, string Step, string StatusCode), TagList> _statusCodeTags = new();
    private readonly ConcurrentDictionary<string, TagList> _metricTags = new();

    // Gauges live as long as the meter, so they are not cleared between sessions.
    private readonly ConcurrentDictionary<(string Name, string? Unit, Type Type), Instrument> _gauges = new();

    /// <summary>
    /// Gets the name of the sink.
    /// </summary>
    public string SinkName => nameof(OpenTelemetrySink);

    /// <summary>
    /// Initializes a new instance of the <see cref="OpenTelemetrySink"/> class with default configuration:
    /// <see cref="OtlpExportProtocol.HttpProtobuf"/> protocol and <see cref="MetricReaderTemporalityPreference.Cumulative"/> temporality preference.
    /// </summary>
    public OpenTelemetrySink()
    {
        _config = new OtlpExporterOptions();
        _config.Protocol = OtlpExportProtocol.HttpProtobuf;
        _readerTemporalityPreference = MetricReaderTemporalityPreference.Cumulative;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="OpenTelemetrySink"/> class using the specified configuration.
    /// </summary>
    /// <param name="config">The OTLP exporter configuration used for OpenTelemetry export.</param>
    /// <param name="readerTemporalityPreference">The temporality preference for the metrics reader. Defaults to <see cref="MetricReaderTemporalityPreference.Cumulative"/>.</param>
    public OpenTelemetrySink(OtlpExporterOptions config, MetricReaderTemporalityPreference readerTemporalityPreference = MetricReaderTemporalityPreference.Cumulative)
    {
        _config = config;
        _readerTemporalityPreference = readerTemporalityPreference;
    }

    /// <summary>
    /// Initializes the OpenTelemetry sink with the NBomber context and configuration.
    /// Sets up the <see cref="MeterProvider"/> and the OTLP metrics exporter.
    /// </summary>
    /// <param name="context">NBomber base context object that provides test and node information.</param>
    /// <param name="infraConfig">Infrastructure configuration section from NBomber configuration.</param>
    /// <returns>A task representing the asynchronous initialization operation.</returns>
    public Task Init(IBaseContext context, IConfiguration infraConfig)
    {
        _logger = context.Logger.ForContext<OpenTelemetrySink>();
        _context = context;
        _eventListener = new OpenTelemetrySdkEventListener(_logger);

        var config = infraConfig?.GetSection("OpenTelemetrySink").Get<OtlpExporterOptions>();
        if (config != null)
            _config = config;

        _customMetricsReader = new EmptyMetricsReader(new OtlpMetricExporter(_config!));
        _customMetricsReader.TemporalityPreference = _readerTemporalityPreference;

        _meter = new Meter("nbomber");

        _meterProvider = Sdk.CreateMeterProviderBuilder()
            .ConfigureResource(x => x.AddService("nbomber"))
            .AddMeter(_meter.Name)
            .AddReader(_customMetricsReader)
            .Build();

        return Task.CompletedTask;
    }

    /// <summary>
    /// Called at the beginning of a test session.
    /// </summary>
    /// <param name="sessionInfo">Information about the test session.</param>
    /// <returns>A completed task.</returns>
    public Task Start(SessionStartInfo sessionInfo)
    {
        ClearTagsCache();
        return Task.CompletedTask;
    }

    /// <summary>
    /// Saves real-time performance metrics (gauges and counters) during the bombing phase.
    /// </summary>
    /// <param name="metrics">The metrics data to record and export through OpenTelemetry.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    public Task SaveRealtimeMetrics(MetricStats metrics)
    {
        RecordMetrics(metrics, OperationType.Bombing);

        _meterProvider?.ForceFlush();

        return Task.CompletedTask;
    }

    /// <summary>
    /// Saves real-time scenario statistics (step performance data) during the bombing phase.
    /// </summary>
    /// <param name="stats">An array of scenario statistics to be recorded.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    public Task SaveRealtimeStats(ScenarioStats[] stats)
    {
        RecordRealtimeStats(stats, OperationType.Bombing);

        _meterProvider?.ForceFlush();

        return Task.CompletedTask;
    }

    /// <summary>
    /// Saves final aggregated statistics and metrics after the test run has completed.
    /// </summary>
    /// <param name="stats">The final node statistics containing metrics and scenario data.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    public Task SaveFinalStats(NodeStats stats)
    {
        RecordRealtimeStats(stats.ScenarioStats, OperationType.Complete);
        RecordMetrics(stats.Metrics, OperationType.Complete);

        _meterProvider?.ForceFlush();

        return Task.CompletedTask;
    }

    /// <summary>
    /// Called when the test session ends.
    /// </summary>
    /// <returns>A completed task.</returns>
    public Task Stop()
    {
        return Task.CompletedTask;
    }

    /// <summary>
    /// Disposes the OpenTelemetry sink by flushing and releasing all managed resources.
    /// </summary>
    public void Dispose()
    {
        _meterProvider?.ForceFlush();

        _meterProvider?.Dispose();
        _customMetricsReader?.Dispose();
        _meter?.Dispose();
        _eventListener?.Dispose();
    }

    private void RecordRealtimeStats(ScenarioStats[] stats, OperationType operationType)
    {
        EnsureTagCache(operationType);

        foreach (var scnStats in stats.Select(AddGlobalInfoSteps))
        {
            RecordStepsStats(scnStats, operationType);
        }
    }

    private ScenarioStats AddGlobalInfoSteps(ScenarioStats stats)
    {
        var globalStepInfo = new StepStats("global information", stats.Ok, stats.Fail, sortIndex: 0);
        stats.StepStats = stats.StepStats.Append(globalStepInfo).ToArray();
        return stats;
    }

    private void RecordStepsStats(ScenarioStats scnStats, OperationType operationType)
    {
        foreach (var stats in scnStats.StepStats)
        {
            var tags = GetStepTags(operationType, scnStats.ScenarioName, stats.StepName);

            RecordGauge("all.request.count", stats.Ok.Request.Count + stats.Fail.Request.Count, tags);
            RecordGauge("all.datatransfer.all", stats.Ok.DataTransfer.AllBytes + stats.Fail.DataTransfer.AllBytes, tags);

            RecordGauge("ok.request.count", stats.Ok.Request.Count, tags);
            RecordGauge("ok.request.rps", stats.Ok.Request.RPS, tags);

            RecordGauge("ok.latency.min", stats.Ok.Latency.MinMs, tags);
            RecordGauge("ok.latency.mean", stats.Ok.Latency.MeanMs, tags);
            RecordGauge("ok.latency.max", stats.Ok.Latency.MaxMs, tags);
            RecordGauge("ok.latency.stddev", stats.Ok.Latency.StdDev, tags);
            RecordGauge("ok.latency.percent50", stats.Ok.Latency.Percent50, tags);
            RecordGauge("ok.latency.percent75", stats.Ok.Latency.Percent75, tags);
            RecordGauge("ok.latency.percent95", stats.Ok.Latency.Percent95, tags);
            RecordGauge("ok.latency.percent99", stats.Ok.Latency.Percent99, tags);

            RecordGauge("ok.datatransfer.min", stats.Ok.DataTransfer.MinBytes, tags);
            RecordGauge("ok.datatransfer.mean", stats.Ok.DataTransfer.MeanBytes, tags);
            RecordGauge("ok.datatransfer.max", stats.Ok.DataTransfer.MaxBytes, tags);
            RecordGauge("ok.datatransfer.all", stats.Ok.DataTransfer.AllBytes, tags);
            RecordGauge("ok.datatransfer.percent50", stats.Ok.DataTransfer.Percent50, tags);
            RecordGauge("ok.datatransfer.percent75", stats.Ok.DataTransfer.Percent75, tags);
            RecordGauge("ok.datatransfer.percent95", stats.Ok.DataTransfer.Percent95, tags);
            RecordGauge("ok.datatransfer.percent99", stats.Ok.DataTransfer.Percent99, tags);

            RecordGauge("fail.request.count", stats.Fail.Request.Count, tags);
            RecordGauge("fail.request.rps", stats.Fail.Request.RPS, tags);

            RecordGauge("fail.latency.min", stats.Fail.Latency.MinMs, tags);
            RecordGauge("fail.latency.mean", stats.Fail.Latency.MeanMs, tags);
            RecordGauge("fail.latency.max", stats.Fail.Latency.MaxMs, tags);
            RecordGauge("fail.latency.stddev", stats.Fail.Latency.StdDev, tags);
            RecordGauge("fail.latency.percent50", stats.Fail.Latency.Percent50, tags);
            RecordGauge("fail.latency.percent75", stats.Fail.Latency.Percent75, tags);
            RecordGauge("fail.latency.percent95", stats.Fail.Latency.Percent95, tags);
            RecordGauge("fail.latency.percent99", stats.Fail.Latency.Percent99, tags);

            RecordGauge("fail.datatransfer.min", stats.Fail.DataTransfer.MinBytes, tags);
            RecordGauge("fail.datatransfer.mean", stats.Fail.DataTransfer.MeanBytes, tags);
            RecordGauge("fail.datatransfer.max", stats.Fail.DataTransfer.MaxBytes, tags);
            RecordGauge("fail.datatransfer.all", stats.Fail.DataTransfer.AllBytes, tags);
            RecordGauge("fail.datatransfer.percent50", stats.Fail.DataTransfer.Percent50, tags);
            RecordGauge("fail.datatransfer.percent75", stats.Fail.DataTransfer.Percent75, tags);
            RecordGauge("fail.datatransfer.percent95", stats.Fail.DataTransfer.Percent95, tags);
            RecordGauge("fail.datatransfer.percent99", stats.Fail.DataTransfer.Percent99, tags);

            RecordGauge("simulation.value", scnStats.LoadSimulationStats.Value, tags);

            RecordStatusCodes(scnStats, stats, operationType);
        }
    }

    private void RecordMetrics(MetricStats stats, OperationType operationType)
    {
        EnsureTagCache(operationType);

        foreach (var counter in stats.Counters)
        {
            var tags = GetMetricTags(operationType, counter.ScenarioName);
            RecordGauge(counter.MetricName, counter.Value, tags, counter.UnitOfMeasure);
        }

        foreach (var gauge in stats.Gauges)
        {
            var tags = GetMetricTags(operationType, gauge.ScenarioName);
            RecordGauge(gauge.MetricName, gauge.Value, tags, gauge.UnitOfMeasure);
        }
    }

    private void RecordStatusCodes(ScenarioStats scnStats, StepStats step, OperationType operationType)
    {
        foreach (var codeStats in step.Ok.StatusCodes.Concat(step.Fail.StatusCodes))
        {
            var tags = GetStatusCodeTags(operationType, scnStats.ScenarioName, step.StepName, codeStats.StatusCode);
            RecordGauge("status_code.count", codeStats.Count, tags);
        }
    }

    private void RecordGauge<T>(string name, T value, in TagList tags, string? measureOfUnit = null) where T : struct
    {
        var gauge = (Gauge<T>)_gauges.GetOrAdd((name, measureOfUnit, typeof(T)),
            static (key, meter) => meter!.CreateGauge<T>(key.Name, key.Unit),
            _meter);

        gauge.Record(value, in tags);
    }

    /// <summary>
    /// Drops the cached tags if they were built for a different operation type.
    /// The operation type changes at most once per session (Bombing -> Complete).
    /// </summary>
    private void EnsureTagCache(OperationType operationType)
    {
        if (_cachedOperationType == operationType) return;

        ClearTagsCache();
        _cachedOperationType = operationType;
    }

    private void ClearTagsCache()
    {
        _cachedOperationType = null;
        _globalTags = null;
        
        _scenarioTags.Clear();
        _stepTags.Clear();
        _statusCodeTags.Clear();
        _metricTags.Clear();
    }

    private Dictionary<string, object?> GetGlobalTags(OperationType operationType) =>
        _globalTags ??= BuildGlobalTags(operationType);

    private KeyValuePair<string, object?>[] GetScenarioTags(OperationType operationType, string scenarioName) =>
        _scenarioTags.GetOrAdd(scenarioName,
            static (scnName, state) => state.Sink.BuildScenarioTags(state.OperationType, scnName),
            (Sink: this, OperationType: operationType));
    
    private TagList GetStepTags(OperationType operationType, string scenarioName, string stepName) =>
        _stepTags.GetOrAdd((scenarioName, stepName),
            static (key, state) =>
            {
                var scnTags = state.Sink.GetScenarioTags(state.OperationType, key.Scenario);
                return new TagList(AppendTag(scnTags, "step", key.Step));
            },
            (Sink: this, OperationType: operationType));

    private TagList GetStatusCodeTags(OperationType operationType, string scenarioName, string stepName, string statusCode) =>
        _statusCodeTags.GetOrAdd((scenarioName, stepName, statusCode),
            static (key, state) =>
            {
                var stepTags = state.Sink.GetStepTags(state.OperationType, key.Scenario, key.Step);
                return new TagList(AppendTag(stepTags.ToArray(), "status_code_status", key.StatusCode));
            },
            (Sink: this, OperationType: operationType));

    private TagList GetMetricTags(OperationType operationType, string scenarioName) =>
        _metricTags.GetOrAdd(scenarioName,
            static (scnName, state) => state.Sink.BuildMetricTags(state.OperationType, scnName),
            (Sink: this, OperationType: operationType));

    private Dictionary<string, object?> BuildGlobalTags(OperationType operationType)
    {
        Dictionary<string, object?> BuildSessionDefaultTags(OperationType operation)
        {
            var nodeInfo = _context!.GetNodeInfo();
            var testInfo = _context!.TestInfo;

            return new Dictionary<string, object?>
            {
                ["session_id"] = testInfo.SessionId,
                ["operation_type"] = operation.ToString(),
                ["node_type"] = nodeInfo.NodeType.ToString(),
                ["test_suite"] = testInfo.TestSuite,
                ["test_name"] = testInfo.TestName
            };
        }

        var tags = BuildSessionDefaultTags(operationType);
        MergeTags(tags, _context!.TestInfo.Tags);
        
        return tags;
    }

    private KeyValuePair<string, object?>[] BuildScenarioTags(OperationType operationType, string scenarioName)
    {
        var globalTags = GetGlobalTags(operationType);

        var tags = new Dictionary<string, object?>(globalTags)
        {
            ["scenario"] = scenarioName
        };

        return tags.ToArray();
    }

    private TagList BuildMetricTags(OperationType operationType, string scenarioName)
    {
        var globalTags = GetGlobalTags(operationType);

        var tags = new Dictionary<string, object?>(globalTags)
        {
            ["scenario"] = scenarioName
        };

        return new TagList(tags.ToArray());
    }

    private void MergeTags(Dictionary<string, object?> target, IReadOnlyDictionary<string, string> tags)
    {
        foreach (var tag in tags)
            target[tag.Key] = tag.Value;
    }
    
    private static KeyValuePair<string, object?>[] AppendTag(KeyValuePair<string, object?>[] tags, string key, object? value)
    {
        var result = new KeyValuePair<string, object?>[tags.Length + 1];
        tags.CopyTo(result, 0);
        result[tags.Length] = new(key, value);

        return result;
    }
}
