using System.Diagnostics;
using System.Diagnostics.Metrics;
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

namespace NBomber.Sinks.OpenTelemetry;

public class OpenTelemetryConfig
{
    public string Url { get; set; } = "http://localhost:4317";
    public OtlpExportProtocol ExportProtocol { get; set; } = OtlpExportProtocol.Grpc;
}

public class OpenTelemetrySink : IReportingSink
{
    private ILogger _logger = null!;
    private IBaseContext _context = null!;
    private MeterProvider _meterProvider = null!;
    private Meter _meter = null!;
    private EmptyMetricsReader _customMetricsReader = null!;
    private OpenTelemetryConfig _config = null!;

    public string SinkName => nameof(OpenTelemetrySink);

    public OpenTelemetrySink() { }

    public OpenTelemetrySink(OpenTelemetryConfig config)
    {
        _config = config;
    }

    public Task Init(IBaseContext context, IConfiguration infraConfig)
    {
        _logger = context.Logger.ForContext<OpenTelemetrySink>();
        _context = context;

        var otlpExporterOptions = new OtlpExporterOptions();
        var config = infraConfig?.GetSection("OpenTelemetrySink").Get<OpenTelemetryConfig>();
        if (config != null)
            _config = config;
        
        try
        {
            otlpExporterOptions.Endpoint = new Uri(_config.Url);
            otlpExporterOptions.Protocol = _config.ExportProtocol;
            _customMetricsReader = new EmptyMetricsReader(new OtlpMetricExporter(otlpExporterOptions));
        }
        catch
        {
            _logger.Error("Reporting Sink {0} has problems with initialization. The problem could be related to invalid config structure.", SinkName);
            throw new InvalidOperationException($"Cannot initialize {SinkName}. Please check the configuration.");
        }

        _meter = new Meter("nbomber");

        _meterProvider = Sdk.CreateMeterProviderBuilder()
            .ConfigureResource(x => x.AddService("nbomber"))
            .AddMeter(_meter.Name)
            .AddReader(_customMetricsReader)
            .Build();

        return Task.CompletedTask;
    }

    public Task Start(SessionStartInfo sessionInfo)
    {
        return Task.CompletedTask;
    }

    public Task SaveRealtimeMetrics(MetricStats metrics)
    {
        RecordMetrics(metrics, OperationType.Bombing);

        _meterProvider.ForceFlush();

        return Task.CompletedTask;
    }

    public Task SaveRealtimeStats(ScenarioStats[] stats)
    {
        RecordRealtimeStats(stats, OperationType.Bombing);

        _meterProvider.ForceFlush();

        return Task.CompletedTask;
    }

    public Task SaveFinalStats(NodeStats stats)
    {
        RecordRealtimeStats(stats.ScenarioStats, OperationType.Complete);
        RecordMetrics(stats.Metrics, OperationType.Complete);

        _meterProvider.ForceFlush();

        return Task.CompletedTask;
    }

    public Task Stop()
    {
        return Task.CompletedTask;
    }

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
        var globalStepInfo = new StepStats("global information", stats.Ok, stats.Fail, 0);
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

        var tags = new Dictionary<string, object?>
        {
            { "test_suite", testInfo.TestSuite },
            { "test_name", testInfo.TestName },
            { "session_id", testInfo.SessionId },
            { "operation_type", operationType },
        };

        foreach (var counter in stats.Counters)
        {
            tags["scenario"] = counter.ScenarioName;
            var counterTags = new TagList(tags.ToArray());
            RecordGauge(counter.MetricName, counter.Value, counterTags, counter.UnitOfMeasure);
        }

        foreach (var gauge in stats.Gauges)
        {
            tags["scenario"] = gauge.ScenarioName;
            var gaugeTags = new TagList(tags.ToArray());
            RecordGauge(gauge.MetricName, gauge.Value, gaugeTags, gauge.UnitOfMeasure);
        }
    }

    private void RecordStatusCodes(ScenarioStats stats, OperationType operationType)
    {
        var testInfo = _context.TestInfo;
        var tags = new Dictionary<string, object?>()
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
