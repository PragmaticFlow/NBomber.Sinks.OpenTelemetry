using OpenTelemetry;
using OpenTelemetry.Metrics;

namespace NBomber.Sinks.OpenTelemetry;

class EmptyMetricsReader(BaseExporter<Metric> exporter) : BaseExportingMetricReader(exporter);
