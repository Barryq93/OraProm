# CM8 WebSphere Observability Setup

Use a **hybrid setup**: keep the OpenTelemetry Java agent for application-level metrics and logs, and use WebSphere `metrics.ear` for PMI-based server internals such as JDBC connection pools, thread pools, and WebSphere dynamic cache metrics.[1][2]

## Recommended split

- Use the OpenTelemetry Java agent for HTTP server/client metrics, JDBC client timings, Kafka/JMS client instrumentation, JVM heap/GC/thread metrics, and log export to Alloy over OTLP HTTP/HTTPS.[2]
- Use WebSphere `metrics.ear` for PMI metrics that the Java agent cannot see well, especially native WebSphere JDBC/JCA connection pool usage, thread pool utilization, and WebSphere caching metrics.[1][3]
- Let Alloy handle both pipelines: scrape `metrics.ear` as Prometheus, and receive Java agent metrics and logs via OTLP HTTP/HTTPS.[3]

## OTel properties file

```properties
# ===== Identity / tags =====
otel.service.name=cm8-was-server
otel.resource.attributes=application=CM8,environment=Prod,service.namespace=cm8,host.name=<host_name>,jvm.name=<jvm_name>,site=dublin

# ===== Signals =====
otel.traces.exporter=none
otel.metrics.exporter=otlp
otel.logs.exporter=otlp

# ===== OTLP transport to local Alloy =====
otel.exporter.otlp.protocol=http/protobuf
otel.exporter.otlp.endpoint=http://localhost:4318
otel.exporter.otlp.metrics.endpoint=http://localhost:4318/v1/metrics
otel.exporter.otlp.logs.endpoint=http://localhost:4318/v1/logs

# If using HTTPS instead of HTTP, switch the endpoints above to https://
# and add one of these depending on cert model:
# otel.exporter.otlp.certificate=/opt/otel/certs/alloy-ca-or-server-cert.pem
# otel.exporter.otlp.client.certificate=/opt/otel/certs/client-cert.pem
# otel.exporter.otlp.client.key=/opt/otel/certs/client-key.pem

# ===== Export interval =====
otel.metric.export.interval=60000

# ===== JVM metrics =====
otel.instrumentation.runtime-metrics.enabled=true

# ===== HTTP metrics =====
otel.instrumentation.http.client.emit-experimental-telemetry=true
otel.instrumentation.http.server.emit-experimental-telemetry=true

# ===== JDBC =====
otel.instrumentation.jdbc.enabled=true
otel.instrumentation.jdbc.statement-sanitizer.enabled=true

# ===== Kafka =====
otel.instrumentation.kafka.enabled=true
otel.instrumentation.kafka.experimental-span-attributes=true

# ===== MQ / JMS =====
otel.instrumentation.jms.enabled=true

# ===== Agent logging =====
otel.javaagent.logging=simple
```

The Java agent supports Java 8, and WebSphere Traditional on IBM JDK 8 is one of the regularly tested combinations.[2]

## What to get from each source

| Area | Stronger source | Why | How they work together |
|---|---|---|---|
| HTTP server and client latency | OTel Java agent [2] | OTel gives standardized request duration metrics and can expose active request telemetry for better app-facing latency analysis. | Use OTel HTTP latency beside PMI thread pool and cache metrics to explain *why* latency rises. |
| JDBC call timing | OTel Java agent [2] | OTel sees database client operations directly from the app path. | Pair JDBC timings with PMI connection pool saturation to separate slow SQL from pool starvation. |
| JVM heap, GC, threads | OTel Java agent [2] | Runtime telemetry is a core Java agent strength. | Combine JVM pressure with PMI thread pool or cache pressure during incidents. |
| WebSphere JDBC/JCA connection pools | `metrics.ear` / PMI [1][3] | WebSphere native pool internals are exposed through PMI, not standard Java pool libraries. | Use pool metrics with OTel HTTP and JDBC metrics to see whether latency is caused by waiting for connections. |
| WebSphere thread pools | `metrics.ear` / PMI [3] | Thread pool usage is a WAS internal metric family. | Correlate thread pool exhaustion with OTel active requests and request duration. |
| WebSphere dynamic cache / caching | `metrics.ear` / PMI [3] | Cache hit/miss and cache capacity behavior are WebSphere internal runtime metrics. | Compare cache efficiency against OTel HTTP and JDBC metrics; good cache hit rates should reduce DB pressure and improve request latency. |
| Kafka and JMS client behavior | OTel Java agent [2] | OTel has client-side messaging instrumentation support. | Use OTel messaging metrics with PMI if MQ-related resource pools are also monitored in WAS. |
| Logs to Loki | OTel Java agent [2] | OTel is the direct log pipeline into Alloy/Loki. | Use logs alongside both OTel and PMI metrics for incident correlation. |

## Key metrics to focus on

### From OTel

- `http.server.request.duration`, `http.server.active_requests`, `http.client.request.duration` for request latency and load.[2]
- `db.client.operation.duration` for JDBC timing visibility.[2]
- `jvm.memory.used`, `jvm.memory.committed`, `jvm.memory.limit`, `jvm.gc.duration`, `jvm.thread.count` for JVM health.[2]

### From `metrics.ear`

- JDBC/JCA connection pool metrics such as `PoolSize`, `FreePoolSize`, `PercentUsed`, `PercentMaxed`, `avgWaitTime`, `avgUseTime`, and `WaitingThreadCount`.[3]
- Thread pool usage metrics from PMI for WebSphere internal capacity tracking.[3]
- Dynamic cache metrics from PMI for WebSphere caching behavior; enable the relevant PMI cache module so `metrics.ear` can expose it in Prometheus format.[3][4]

## Recommendation

HTTP metrics are generally **better from OTel** because they are application-aware and better suited to latency analysis at the request level.[2] Connection pools, thread pools, and WebSphere caching are generally **better from `metrics.ear`** because they are native WAS internals surfaced through PMI.[3][1]

## Alloy pattern

- Scrape `http://<host>:<port>/metrics` or the scoped `/metrics/<node>/<server>` path from WebSphere `metrics.ear` as Prometheus.[3]
- Receive OTLP metrics and logs from the Java agent on Alloy's HTTP receiver, typically port 4318.[2]
- Forward Prometheus data to Prometheus/remote_write and logs to Loki from Alloy.[3]

## Optional WAS setting

To include WebSphere cell, node, and server labels in Prometheus output, set the following JVM custom property to `true`.[3]

```properties
com.ibm.ws.pmi.prometheus.includeCellNodeServerLabels=true
```
