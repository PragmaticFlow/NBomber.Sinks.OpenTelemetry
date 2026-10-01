namespace NBomber.Sinks.OpenTelemetry.Tests;

public class OtelCollectorFixture : IAsyncLifetime
{
    private static readonly HttpClient Http = new() { Timeout = TimeSpan.FromSeconds(2) };
    private static readonly TimeSpan ReadyTimeout = TimeSpan.FromSeconds(30);

    public Uri OtlpGrpcEndpoint { get; } = new("http://localhost:4317");
    public Uri OtlpHttpEndpoint { get; } = new("http://localhost:4318/v1/metrics");
    public Uri PrometheusEndpoint { get; } = new("http://localhost:9090");
    private Uri CollectorHealthCheckEndpoint { get; } = new("http://localhost:13133");

    public async ValueTask InitializeAsync()
    {
        await WaitUntilReady("OTEL Collector", CollectorHealthCheckEndpoint);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static async Task WaitUntilReady(string service, Uri url)
    {
        var deadline = DateTime.UtcNow + ReadyTimeout;

        while (true)
        {
            try
            {
                using var response = await Http.GetAsync(url);
                if (response.IsSuccessStatusCode)
                    return;
            }
            catch (HttpRequestException) { }
            catch (TaskCanceledException) { }

            if (DateTime.UtcNow > deadline)
                throw new InvalidOperationException(
                    $"{service} is not available at {url}. Start the test infrastructure first: " +
                    "'docker compose -f tests/NBomber.Sinks.OpenTelemetry.Tests/docker-compose.yaml up -d --wait'.");

            await Task.Delay(TimeSpan.FromSeconds(1));
        }
    }
}
