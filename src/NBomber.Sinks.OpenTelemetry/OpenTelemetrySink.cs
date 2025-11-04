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
/// to OpenTelemetry-compatible systems using the OTLP protocol (e.g., Prometheus, Grafana, Tempo).
/// </summary>
public class OpenTelemetrySink : IReportingSink
{
    private ILogger _logger = null!;
    private IBaseContext _context = null!;
    private MeterProvider _meterProvider = null!;
    private Meter _meter = null!;
    private EmptyMetricsReader _customMetricsReader = null!;
    private OtlpExporterOptions _config = null!;

    /// <summary>
    /// Gets the name of the sink.
    /// </summary>
    public string SinkName => nameof(OpenTelemetrySink);

    /// <summary>
    /// Initializes a new instance of the <see cref="OpenTelemetrySink"/> class with default configuration.
    /// </summary>
    public OpenTelemetrySink()
    {
        _config = new OtlpExporterOptions();
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="OpenTelemetrySink"/> class using the specified configuration.
    /// </summary>
    /// <param name="config">The OTLP exporter configuration used for OpenTelemetry export.</param>
    public OpenTelemetrySink(OtlpExporterOptions config)
    {
        _config = config;
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

        var config = infraConfig?.GetSection("OpenTelemetrySink").Get<OtlpExporterOptions>();
        if (config != null)
            _config = config;

        _customMetricsReader = new EmptyMetricsReader(new OtlpMetricExporter(_config));
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
        _meterProvider.ForceFlush();
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
        _meterProvider.ForceFlush();
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
        _meterProvider.ForceFlush();
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
        _meterProvider.ForceFlush();
        _meterProvider.Dispose();
        _customMetricsReader.Dispose();
        _meter.Dispose();
    }

    private void RecordRealtimeStats(ScenarioStats[] stats, OperationType operationType)
    {
        foreach (var scnStats in stats.Select(AddGlobalInfoSteps))
        {
            RecordStepsStats(scnStats, operationType);
        }

        foreach (var x in stats)
        {
            RecordStatusCodes(x, operationType);
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
        var testInfo = _context.TestInfo;

        var commonTags = new Dictionary<string, object?>
        {
            { "test_suite", testInfo.TestSuite },
            { "test_name", testInfo.TestName },
            { "scenario", scnStats.ScenarioName },
            { "session_id", testInfo.SessionId },
            { "operation_type", operationType }
        };

        foreach (var stats in scnStats.StepStats)
        {
            commonTags["step"] = stats.StepName;
            var tags = new TagList(commonTags.ToArray());

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
        }
    }

    private void RecordMetrics(MetricStats stats, OperationType operationType)
    {
        var testInfo = _context.TestInfo;

        var countersTags = stats.Counters.GroupBy(x => x.ScenarioName)
            .ToDictionary(x => x.Key, v =>
            {
                var tags = new KeyValuePair<string, object?>[]
                {
                    new("test_suite", testInfo.TestSuite),
                    new("test_name", testInfo.TestName),
                    new("session_id", testInfo.SessionId),
                    new("operation_type", operationType),
                    new("scenario", v.Key)
                };

                return new TagList(tags);
            });

        var gaugesTags = stats.Gauges.GroupBy(x => x.ScenarioName)
            .ToDictionary(x => x.Key, v =>
            {
                var tags = new KeyValuePair<string, object?>[]
                {
                    new("test_suite", testInfo.TestSuite),
                    new("test_name", testInfo.TestName),
                    new("session_id", testInfo.SessionId),
                    new("operation_type", operationType),
                    new("scenario", v.Key)
                };

                return new TagList(tags);
            });

        foreach (var counter in stats.Counters)
        {
            RecordGauge(counter.MetricName, counter.Value, countersTags[counter.ScenarioName], counter.UnitOfMeasure);
        }

        foreach (var gauge in stats.Gauges)
        {
            RecordGauge(gauge.MetricName, gauge.Value, gaugesTags[gauge.ScenarioName], gauge.UnitOfMeasure);
        }
    }

    private void RecordStatusCodes(ScenarioStats stats, OperationType operationType)
    {
        var testInfo = _context.TestInfo;
        var tags = new Dictionary<string, object?>
        {
            { "test_name", testInfo.TestName },
            { "test_suite", testInfo.TestSuite },
            { "scenario", stats.ScenarioName },
            { "operation_type", operationType },
        };

        foreach (var codeStats in stats.Ok.StatusCodes.Concat(stats.Fail.StatusCodes))
        {
            tags["status_code_status"] = codeStats.StatusCode;
            var tagList = new TagList(tags.ToArray());

            RecordGauge("status_code.count", codeStats.Count, tagList);
        }
    }

    private void RecordGauge<T>(string name, T value, TagList tags, string? measureOfUnit = null) where T : struct
    {
        var gauge = _meter.CreateGauge<T>(name, measureOfUnit);
        gauge.Record(value, tags);
    }
}
