using System.Diagnostics.Tracing;
using Serilog;

namespace NBomber.Sinks.OpenTelemetry;

internal class OpenTelemetrySdkEventListener(ILogger logger) : EventListener
{
    protected override void OnEventSourceCreated(EventSource eventSource)
    {
        if (eventSource.Name.StartsWith("OpenTelemetry", StringComparison.OrdinalIgnoreCase))
        {
            EnableEvents(eventSource, EventLevel.Error);
        }  
    }

    protected override void OnEventWritten(EventWrittenEventArgs eventData)
    {
        if (!eventData.EventSource.Name.StartsWith("OpenTelemetry", StringComparison.OrdinalIgnoreCase))
            return;

        var payload = eventData.Payload != null
            ? string.Join(", ", eventData.Payload)
            : string.Empty;

        logger.Error(
            "OpenTelemetrySink [{Source}] {EventName}: {Payload}",
            eventData.EventSource.Name,
            eventData.EventName,
            payload);
    }
}
