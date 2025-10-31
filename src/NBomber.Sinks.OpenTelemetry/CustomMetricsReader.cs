using OpenTelemetry;
using OpenTelemetry.Metrics;

namespace NBomber.Sinks.OpenTelemetry
{
    class CustomMetricsReader : BaseExportingMetricReader
    {
        public CustomMetricsReader(BaseExporter<Metric> exporter) : base(exporter) { }
    }
}
