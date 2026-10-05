using NBomber.Contracts;
using NBomber.Contracts.Stats;
using NBomber.CSharp;
using NBomber.Sinks.OpenTelemetry.Tests.Infra;
using OpenTelemetry.Exporter;
using Shouldly;

namespace NBomber.Sinks.OpenTelemetry.Tests;

public class OpenTelemetrySinkTests(OtelCollectorFixture fixture) : IClassFixture<OtelCollectorFixture>
{
    private const string ScenarioName = "e2e_scenario";
    private const string StepName = "step_1";
    private const string CounterName = "e2e-custom-counter";
    private const string GaugeName = "e2e-custom-gauge";
    private const double GaugeValue = 42.5;

    private readonly PrometheusClient _metrics = new(fixture.PrometheusEndpoint);

    [Theory]
    [InlineData(OtlpExportProtocol.Grpc)]
    [InlineData(OtlpExportProtocol.HttpProtobuf)]
    public async Task ReportingSink_should_write_final_scenario_stats(OtlpExportProtocol protocol)
    {
        var testName = CreateTestName(protocol);
        var sink = CreateSink(protocol);

        var stats = RunLoadTest(sink, testName);

        var scnStats = stats.ScenarioStats.Get(ScenarioName);
        var stepStats = scnStats.StepStats.First(x => x.StepName == StepName);
        scnStats.Ok.Request.Count.ShouldBeGreaterThan(0);

        var scenarioLabels = GenerateLabels(testName, OperationType.Complete, new() { ["step"] = "global information" });
        var stepLabels = GenerateLabels(testName, OperationType.Complete, new() { ["step"] = StepName });

        var scnOkCount = await _metrics.WaitForSample("ok.request.count", scenarioLabels);
        scnOkCount.Value.ShouldBe(scnStats.Ok.Request.Count);

        var scnFailCount = await _metrics.WaitForSample("fail.request.count", scenarioLabels);
        scnFailCount.Value.ShouldBe(scnStats.Fail.Request.Count);

        var stepOkCount = await _metrics.WaitForSample("ok.request.count", stepLabels);
        stepOkCount.Value.ShouldBe(stepStats.Ok.Request.Count);

        var stepLatencyMax = await _metrics.WaitForSample("ok.latency.max", stepLabels);
        stepLatencyMax.Value.ShouldBe(stepStats.Ok.Latency.MaxMs, tolerance: 0.001);

        var statusCodeLabels = GenerateLabels(testName, OperationType.Complete,
            new() { ["step"] = "global information", ["status_code_status"] = "200" });
        
        var statusCodeCount = await _metrics.WaitForSample("status_code.count", statusCodeLabels);
        statusCodeCount.Value.ShouldBe(scnStats.Ok.StatusCodes.First(x => x.StatusCode == "200").Count);
    }

    [Theory]
    [InlineData(OtlpExportProtocol.Grpc)]
    [InlineData(OtlpExportProtocol.HttpProtobuf)]
    public async Task ReportingSink_should_write_realtime_stats(OtlpExportProtocol protocol)
    {
        var testName = CreateTestName(protocol);
        var sink = CreateSink(protocol);

        RunLoadTest(sink, testName);

        var labels = GenerateLabels(testName, OperationType.Bombing, new() { ["step"] = StepName });

        var okCount = await _metrics.WaitForSample("ok.request.count", labels);
        okCount.Value.ShouldBeGreaterThan(0);
    }

    [Theory]
    [InlineData(OtlpExportProtocol.Grpc)]
    [InlineData(OtlpExportProtocol.HttpProtobuf)]
    public async Task ReportingSink_should_write_custom_metrics(OtlpExportProtocol protocol)
    {
        var testName = CreateTestName(protocol);
        var sink = CreateSink(protocol);

        var stats = RunLoadTest(sink, testName);

        var counterStats = stats.Metrics.Counters.First(x => x.MetricName == CounterName);
        var gaugeStats = stats.Metrics.Gauges.First(x => x.MetricName == GaugeName);
        counterStats.Value.ShouldBeGreaterThan(0);

        var labels = GenerateLabels(testName, OperationType.Complete);

        var counter = await _metrics.WaitForSample(CounterName, labels);
        counter.Value.ShouldBe(counterStats.Value);

        var gauge = await _metrics.WaitForSample(GaugeName, labels);
        gaugeStats.Value.ShouldBe(GaugeValue);
        gauge.Value.ShouldBe(gaugeStats.Value);
    }

    private static NodeStats RunLoadTest(OpenTelemetrySink sink, string testName)
    {
        var counter = Metric.CreateCounter(CounterName, unitOfMeasure: "MB");
        var gauge = Metric.CreateGauge(GaugeName, unitOfMeasure: "KB");

        var scenario = Scenario.Create(ScenarioName, async context =>
        {
            await Step.Run(StepName, context, async () =>
            {
                await Task.Delay(10);

                counter.Add(1);
                gauge.Set(GaugeValue);

                return Response.Ok(statusCode: "200", sizeBytes: 100);
            });

            return Response.Ok();
        })
        .WithInit(ctx =>
        {
            ctx.RegisterMetric(counter);
            ctx.RegisterMetric(gauge);
            return Task.CompletedTask;
        })
        .WithoutWarmUp()
        .WithLoadSimulations(
            // longer than the reporting interval, so at least one realtime report is sent
            Simulation.Inject(rate: 10, interval: TimeSpan.FromSeconds(1), during: TimeSpan.FromSeconds(7))
        );

        return NBomberRunner
            .RegisterScenarios(scenario)
            .WithTestSuite("e2e")
            .WithTestName(testName)
            .WithReportingInterval(TimeSpan.FromSeconds(5))
            .WithoutReports()
            .WithReportingSinks(sink)
            .Run();
    }

    private OpenTelemetrySink CreateSink(OtlpExportProtocol protocol)
    {
        var config = new OtlpExporterOptions
        {
            Protocol = protocol,
            Endpoint = protocol == OtlpExportProtocol.Grpc
                ? fixture.OtlpGrpcEndpoint
                : fixture.OtlpHttpEndpoint
        };

        return new OpenTelemetrySink(config);
    }

    private static string CreateTestName(OtlpExportProtocol protocol) => $"{protocol}_{Guid.NewGuid():N}";

    private static Dictionary<string, string> GenerateLabels(string testName, OperationType operationType,
        Dictionary<string, string>? customLabels = null)
    {
        var labels = new Dictionary<string, string>
        {
            ["job"] = "nbomber", // service.name resource attribute
            ["test_suite"] = "e2e",
            ["test_name"] = testName,
            ["scenario"] = ScenarioName,
            ["operation_type"] = operationType.ToString()
        };

        if (customLabels != null)
        {
            foreach (var (key, value) in customLabels)
                labels[key] = value;
        }

        return labels;
    }
}
