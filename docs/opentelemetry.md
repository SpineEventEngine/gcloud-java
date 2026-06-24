# Enabling OpenTelemetry tracing on Google Cloud

Spine records the handling of every signal (a command or an event) by an entity as a
distributed trace. Until now, `gcloud-jvm` reported these traces to Google Cloud Trace
through the [`stackdriver-trace`](../stackdriver-trace) module. That module is being
**retired**: Google now recommends [OpenTelemetry][otel] over the Stackdriver client
libraries, and `core-jvm` ships an OpenTelemetry-based replacement — the
[`server-otel`][server-otel] module, introduced in [core-jvm#1633][pr].

This guide walks you through enabling the new tracer in a Spine server application and
routing its traces to Google Cloud.

> ⚠️ **Experimental.** `server-otel` is built on the alpha
> [Kotlin OpenTelemetry API][otel-kotlin] (`io.opentelemetry.kotlin`). Its public API is
> marked with both `@io.spine.server.trace.otel.ExperimentalOtelTracing` and
> `@io.spine.annotation.Experimental`, and may change in a backward-incompatible way, or be
> removed, in a future release. Opt in explicitly, as shown below.

## How it works

`server-otel` implements the Spine Trace API (`io.spine.server.trace`) on top of
OpenTelemetry. Each handler invocation becomes a single OpenTelemetry **span**, and all
signals of one causal chain are grouped under a single **trace**. The module is
**backend-agnostic**: it maps Spine signals onto spans and leaves the export to an
`OpenTelemetry` instance that *you* create, configure, and own. To send traces to Google
Cloud you configure that instance with an exporter that reaches Cloud Trace — directly, or
(recommended) through an [OpenTelemetry Collector][collector].

Each span carries the following attributes, which you can filter on in the Cloud Trace
Explorer:

| Attribute               | Meaning                                      |
|-------------------------|----------------------------------------------|
| `spine.bounded_context` | the bounded context that handled the signal  |
| `spine.tenant`          | the tenant, as compact JSON                  |
| `spine.entity.id`       | the ID of the receiving entity               |
| `spine.entity.type`     | the type URL of the receiving entity         |
| `spine.signal.id`       | the ID of the handled signal                 |
| `spine.signal.type`     | the type URL of the handled signal           |

Tracing applies to entities — aggregates, process managers, and projections. Standalone
commanders, reactors, and subscribers are not traced.

## Before you start

- A Spine server application built on [`core-jvm`][core-jvm] and `gcloud-jvm`, running on
  Java 17 or higher.
- A Google Cloud project with the trace-ingestion API enabled. The modern path is the
  **Telemetry (OTLP) API**; the legacy path is the **Cloud Trace API**:

  ```bash
  # Recommended: the OpenTelemetry (OTLP) ingestion endpoint.
  gcloud services enable telemetry.googleapis.com

  # Or, for the legacy in-process Cloud Trace exporter (see Option B).
  gcloud services enable cloudtrace.googleapis.com
  ```

- Credentials available to the exporting process via
  [Application Default Credentials][adc] (ADC). Locally, run
  `gcloud auth application-default login`; on Google Cloud, the workload's service account
  is used automatically. Grant the principal the IAM roles for the API in use:
    - **Telemetry (OTLP) API:** `roles/telemetry.tracesWriter` (add `roles/logging.logWriter`
      and `roles/monitoring.metricWriter` if you also export logs or metrics). You must also
      configure a quota project and grant `roles/serviceusage.serviceUsageConsumer` on it; see
      the [Telemetry API overview][telemetry-api].
    - **Legacy Cloud Trace API:** `roles/cloudtrace.agent`.

  For Option A the credentials live on the Collector host, so these apply to whichever
  principal the Collector authenticates as.

The examples below are in Kotlin: the `OtelTracerFactory` API and the Kotlin OpenTelemetry
SDK configuration DSL are idiomatic only in Kotlin. If your application is otherwise written
in Java, keep the tracing setup in a small Kotlin source set.

## Step 1 — Add the dependencies

`spine-server-otel` brings in the Kotlin OpenTelemetry **API** transitively, but not the
SDK or any exporter — you choose those to match how you export to Google Cloud (Step 2).

```kotlin
// build.gradle.kts
val spineVersion: String by extra        // the `core-jvm` version that ships `server-otel`
val otelKotlinVersion = "0.4.0"

dependencies {
    // The Spine OpenTelemetry tracer.
    implementation("io.spine:spine-server-otel:$spineVersion")

    // The Kotlin OpenTelemetry SDK: `core` provides the SDK types (span processors,
    // exporters), and `implementation` adds the `createOpenTelemetry { }` entry point.
    implementation("io.opentelemetry.kotlin:core:$otelKotlinVersion")
    implementation("io.opentelemetry.kotlin:implementation:$otelKotlinVersion")

    // An OTLP/HTTP span exporter (used by Option A, below).
    implementation("io.opentelemetry.kotlin:exporters-otlp:$otelKotlinVersion")
}
```

Because the underlying API is experimental, opt in to its markers project-wide so you do not
have to annotate every declaration:

```kotlin
// build.gradle.kts
kotlin {
    compilerOptions {
        optIn.add("io.opentelemetry.kotlin.ExperimentalApi")
        optIn.add("io.spine.server.trace.otel.ExperimentalOtelTracing")
    }
}
```

## Step 2 — Build an `OpenTelemetry` instance that exports to Google Cloud

This is where the Google-Cloud-specific configuration lives. Pick one of the options below.

### Option A — Through an OpenTelemetry Collector (recommended)

This is Google's recommended production architecture: the application exports spans over
OTLP to a [Collector][collector], and the Collector authenticates to Google Cloud and
forwards the traces. The application stays decoupled from the backend, and credentials live
with the Collector rather than in the app.

The application side is pure Kotlin and needs no Google Cloud libraries — it just sends OTLP
over HTTP to the Collector (here, the default OTLP/HTTP port on `localhost`):

```kotlin
import io.opentelemetry.kotlin.OpenTelemetry
import io.opentelemetry.kotlin.createOpenTelemetry
import io.opentelemetry.kotlin.tracing.export.SpanProcessor
import io.opentelemetry.kotlin.tracing.export.batchSpanProcessor
import io.opentelemetry.kotlin.tracing.export.otlpHttpSpanExporter

/** The batching processor, retained so it can be flushed and shut down on exit (Step 4). */
lateinit var spanProcessor: SpanProcessor

/** Builds an `OpenTelemetry` instance that batches spans to a local Collector. */
fun collectorOpenTelemetry(
    endpoint: String = "http://localhost:4318",
): OpenTelemetry =
    createOpenTelemetry {
        tracerProvider {
            export {
                batchSpanProcessor(otlpHttpSpanExporter(endpoint)).also { spanProcessor = it }
            }
        }
    }
```

A minimal Collector configuration that receives OTLP and exports to Google Cloud:

```yaml
# collector.yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318

exporters:
  googlecloud:
    project: YOUR_GCP_PROJECT_ID

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [googlecloud]
```

The `googlecloud` exporter ships with the
[`contrib` Collector distribution][collector-contrib]; the Collector host authenticates
through ADC. See Google's [OTLP ingestion guide][gcp-otlp] for routing through the Telemetry
API (`telemetry.googleapis.com`) instead, which Google recommends for high trace volumes.

### Option B — Direct in-process export (no Collector)

When you cannot run a Collector, export to Google Cloud from within the application. The
cleanest self-contained option uses Google's OpenTelemetry **Java** exporter, which handles
ADC authentication for you. Because that exporter targets the OpenTelemetry *Java* SDK, build
a Java `OpenTelemetrySdk` and bridge it to the Kotlin API with `toOtelKotlinApi()` from the
[`compat`][otel-kotlin] module.

Add the bridge and the Java exporter (which pulls in the OpenTelemetry Java SDK transitively,
so the SDK types used below resolve without a separate dependency):

```kotlin
// build.gradle.kts
dependencies {
    // Bridges an OpenTelemetry Java SDK instance to the Kotlin API.
    implementation("io.opentelemetry.kotlin:compat:$otelKotlinVersion")

    // Google's OpenTelemetry exporter for Cloud Trace (Java). Check Maven Central
    // for the latest version.
    implementation("com.google.cloud.opentelemetry:exporter-trace:0.36.0")
}
```

Build the instance:

```kotlin
import com.google.cloud.opentelemetry.trace.TraceExporter
import io.opentelemetry.kotlin.OpenTelemetry
import io.opentelemetry.kotlin.toOtelKotlinApi
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor

/** Builds an `OpenTelemetry` instance that exports spans straight to Cloud Trace. */
fun googleCloudOpenTelemetry(): OpenTelemetry {
    // `createWithDefaultConfiguration()` picks up the project ID and credentials from
    // Application Default Credentials. Use `createWithConfiguration(...)` to set them
    // explicitly.
    val exporter = TraceExporter.createWithDefaultConfiguration()
    val javaSdk = OpenTelemetrySdk.builder()
        .setTracerProvider(
            SdkTracerProvider.builder()
                .addSpanProcessor(BatchSpanProcessor.builder(exporter).build())
                .build()
        )
        .build()
    return javaSdk.toOtelKotlinApi()
}
```

> Google now steers new and existing users toward the OTLP Telemetry API over this
> in-process Cloud Trace exporter. The exporter remains supported, but for new deployments
> prefer Option A, or configure the Java SDK with an OTLP exporter aimed at
> `telemetry.googleapis.com` — see Google's [OTLP ingestion guide][gcp-otlp].

## Step 3 — Register the tracer with the server environment

Wire the `OpenTelemetry` instance from Step 2 into an `OtelTracerFactory`, and register that
factory with the server environment. The example registers it for `DefaultMode` — the
built-in environment that is active whenever the server is not running tests; register it
under your own `io.spine.environment.EnvironmentType` instead if your application defines one.
Do this once, at start-up.

```kotlin
import io.opentelemetry.kotlin.OpenTelemetry
import io.spine.environment.DefaultMode
import io.spine.server.ServerEnvironment
import io.spine.server.trace.otel.OtelTracerFactory

fun enableTracing(openTelemetry: OpenTelemetry) {
    val factory = OtelTracerFactory(openTelemetry)
    ServerEnvironment
        .`when`(DefaultMode::class.java)
        .use(factory)
}
```

The instrumentation scope name defaults to `io.spine.server.trace.otel`. To override it, pass
a second argument:
`OtelTracerFactory(openTelemetry, instrumentationScopeName = "my.app.tracing")`.

## Step 4 — Manage the `OpenTelemetry` lifecycle

The `OpenTelemetry` instance is **owned by you**, not by Spine. `OtelTracerFactory.close()`
deliberately does *not* shut it down, so you must flush and close it yourself when the server
stops — otherwise spans buffered by the batch processor can be lost.

- **Option B (Java SDK):** keep the `OpenTelemetrySdk` reference and call `javaSdk.close()`
  on shutdown; this flushes and shuts down its span processors.
- **Option A (Kotlin SDK):** flush and shut down the `SpanProcessor` captured in Step 2.
  Both `forceFlush()` and `shutdown()` are `suspend` functions, so call them from a coroutine
  — for example, from a JVM shutdown hook:

  ```kotlin
  Runtime.getRuntime().addShutdownHook(Thread {
      runBlocking {
          spanProcessor.forceFlush()
          spanProcessor.shutdown()
      }
  })
  ```

  Call `forceFlush()` before `shutdown()`, and tie this into however your application shuts
  down (a shutdown hook as above, or your framework's lifecycle callbacks).

## Verifying

Start the server, exercise a command or event handler, then open the
[Cloud Trace Explorer][trace-explorer] for your project. Within a minute or two you should
see traces named `"<Receiver> handles <Signal>"`. Filter by any of the `spine.*` attributes
listed above to find a specific bounded context, entity, or signal type.

## Migrating from `stackdriver-trace`

If your application currently uses the `stackdriver-trace` module:

1. **Swap the dependency.** Remove `io.spine.gcloud:spine-stackdriver-trace` and add the
   dependencies from Step 1.
2. **Replace the factory wiring.** The old setup built a `StackdriverTracerFactory` with a
   GCP project ID and a gRPC call context:

   ```kotlin
   // Before — Stackdriver:
   val factory = StackdriverTracerFactory.newBuilder()
       .setGcpProjectId("my-gcp-project")
       .setCallContext(callContext)
       .build()
   ServerEnvironment.`when`(DefaultMode::class.java).use(factory)
   ```

   Replace it with an `OtelTracerFactory` over an `OpenTelemetry` instance (Steps 2–3). The
   project ID and credentials now live in the exporter or Collector configuration rather than
   on the factory.
3. **Note the behavioral changes:**
   - Each handler invocation is recorded as its **own span**, not as an event on a shared
     span. (OpenTelemetry [deprecated the Span Events API][span-events] in March 2026.)
   - Span attributes are namespaced under `spine.*` (see the table above).
   - Export is no longer Google-Cloud-specific: the same tracer can target any
     OpenTelemetry backend by reconfiguring the `OpenTelemetry` instance.

## Further reading

- [`spine-server-otel` README][server-otel] — the module reference.
- [Kotlin OpenTelemetry documentation][otel-kotlin] — SDK, exporters, and the configuration DSL.
- [Trace using OpenTelemetry — Google Cloud][gcp-otlp] — exporting to Cloud Trace.

[otel]: https://opentelemetry.io/
[otel-kotlin]: https://opentelemetry.io/docs/languages/kotlin/
[server-otel]: https://github.com/SpineEventEngine/core-jvm/blob/master/server-otel/README.md
[pr]: https://github.com/SpineEventEngine/core-jvm/pull/1633
[core-jvm]: https://github.com/SpineEventEngine/core-jvm/
[collector]: https://opentelemetry.io/docs/collector/
[collector-contrib]: https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/googlecloudexporter
[adc]: https://cloud.google.com/docs/authentication/application-default-credentials
[gcp-otlp]: https://cloud.google.com/trace/docs/otlp
[telemetry-api]: https://cloud.google.com/stackdriver/docs/reference/telemetry/overview
[trace-explorer]: https://cloud.google.com/trace/docs/finding-traces
[span-events]: https://opentelemetry.io/blog/2026/deprecating-span-events/
