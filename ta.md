# Technical Design Blueprint: Enterprise Avro Integration Middleware for Tungsten TotalAgility (KTA)

**Target Environment:** Windows Server 2022 | .NET 8 / .NET 9

**Hosting Platform:** IIS 10 with ASP.NET Core Hosting Bundle

**Purpose:** Secure, reusable middleware for publishing Tungsten TotalAgility events to Apache Kafka using Apache Avro serialization.

**Security Model:** Mutual TLS (mTLS) + Certificate Authorization + SASL_SSL/SCRAM-SHA-512

**Serialization Format:** Apache Avro (Exclusive)

**Schema Management:** Confluent Schema Registry (Dynamic Resolution with In-Memory Caching)

---

# 1. Solution Overview

The middleware provides a secure translation layer between Tungsten TotalAgility (KTA) and Apache Kafka.

Rather than allowing KTA to communicate directly with Kafka, the middleware is responsible for:

* Authenticating inbound requests
* Validating client certificates
* Validating payloads against Avro schemas
* Performing Avro serialization
* Publishing to Kafka
* Returning success or validation errors to KTA

The middleware intentionally supports **Apache Avro only**, reducing complexity and ensuring consistent governance across all event contracts.

---

# 2. High-Level Architecture

```text
                     Tungsten TotalAgility
                     Integration Server
                              │
                    HTTPS + Mutual TLS
                              │
                              ▼
                ASP.NET Core Integration Middleware
                              │
        ┌────────────────────────────────────────────┐
        │ Certificate Authentication                │
        │ Certificate Authorization                 │
        │ Payload Validation                        │
        │ Avro Serialization                        │
        │ Kafka Publishing                          │
        └────────────────────────────────────────────┘
                     │                    │
                     │                    │
                     ▼                    ▼
          Schema Registry         Apache Kafka Cluster
```

---

# 3. Request Processing Pipeline

Every request follows the same deterministic processing pipeline.

```text
Receive HTTPS Request

↓

Validate TLS Handshake

↓

Validate Client Certificate

↓

Authorize Certificate

↓

Resolve Schema (Memory Cache)

↓

Validate JSON Against Avro Schema

↓

Create GenericRecord

↓

Serialize to Avro

↓

Publish to Kafka

↓

Return HTTP Response
```

No message is published unless every validation stage completes successfully.

---

# 4. Startup Initialisation

The middleware follows a fail-fast startup strategy.

During application startup it performs the following operations:

1. Load application configuration.
2. Load all configured schema subjects.
3. Connect to the Schema Registry.
4. Download the latest version of every required schema.
5. Parse each schema into compiled Avro objects.
6. Store parsed schemas in memory.
7. Initialise the Kafka Producer.
8. Verify Kafka connectivity.
9. Begin accepting requests.

If any of these operations fail, the application does **not** start.

This prevents the service from accepting requests while unable to publish messages.

---

# 5. Schema Management

The middleware retrieves the latest schema for each configured subject during startup.

Each cached entry contains:

* Schema Subject
* Schema Version
* Schema ID
* Parsed Avro Schema
* Generic Record Mapping Metadata
* Last Refresh Timestamp

The cache is stored in memory using a thread-safe collection.

Example:

```csharp
ConcurrentDictionary<string, CachedSchema>
```

No Schema Registry lookup occurs during normal request processing.

---

# 6. Automatic Schema Refresh

Schema evolution is supported without restarting the application.

A hosted background service periodically:

1. Queries the Schema Registry.
2. Compares the latest version number.
3. Downloads updated schemas.
4. Parses them.
5. Atomically replaces the cached version.

This provides:

* zero downtime
* support for schema evolution
* no per-request network latency
* immediate use of new schemas after refresh

---

# 7. Payload Validation

Every incoming payload is validated before serialization.

Validation includes:

* Required fields
* Data types
* Nullable unions
* Arrays
* Maps
* Nested records
* Enumerations
* Default values
* Logical types

  * timestamp-millis
  * date
  * decimal
  * UUID

Only payloads that fully conform to the cached Avro schema are published.

Example response:

```json
{
  "error": "Schema validation failed",
  "field": "InvoiceNumber",
  "reason": "Required field missing"
}
```

Invalid requests return an appropriate client error and are never published.

---

# 8. Security Architecture

The middleware is designed to be accessible **only** by the Tungsten TotalAgility Integration Server.

### Mutual TLS

IIS is configured with:

* Require HTTPS
* Require Client Certificates

Connections without a trusted client certificate are rejected before reaching ASP.NET Core.

### Trusted Certificate Authority

Only certificates issued by the organisation's internal Certificate Authority are trusted.

Public Certificate Authorities are not accepted.

### Certificate Validation

The application validates:

* Certificate chain
* Certificate validity dates
* Issuing Certificate Authority
* Enhanced Key Usage (where applicable)
* Subject Alternative Name or Common Name
* Optional certificate thumbprint pinning

### Certificate Authorization

Only explicitly authorised certificates may invoke the service.

Example:

```yaml
SecuritySettings:

  AllowedCertificates:

    - CN: kta-core-worker.yourdomain.local
```

Requests from any other certificate are rejected with HTTP 403.

---

# 9. Network Security

The middleware is intended for internal use only.

Recommended deployment:

* Internal network only
* No Internet exposure
* Windows Firewall restricted to the KTA Integration Server IP address(s)
* HTTPS only
* TLS 1.2 or later

If hosted on the same server as KTA, bind the application only to the local interface where practical.

---

# 10. Kafka Connectivity

Kafka communication uses:

* SSL encryption
* SASL_SSL
* SCRAM-SHA-512 authentication

The Kafka account follows the principle of least privilege.

Permissions should include:

* Produce to authorised topics
* Read Schema Registry (if secured)

The account should not:

* Consume messages
* Create topics
* Delete topics
* Perform cluster administration

---

# 11. Kafka Producer Lifecycle

The Kafka Producer is created once during application startup.

A singleton producer instance is reused for every request.

The producer is never recreated per request.

This minimises connection overhead and maximises throughput.

---

# 12. Configuration Management

Configuration is stored in YAML using:

NetEscapades.Configuration.Yaml

Example:

```yaml
Logging:
  LogLevel:
    Default: Information
    Microsoft.AspNetCore: Warning

SecuritySettings:
  AllowedCertificateCn: "kta-core-worker.yourdomain.local"

SchemaRegistrySettings:
  Url: "https://yourdomain.local"

SchemaSubjects:
  - customer-value
  - invoice-value
  - audit-value

KafkaSettings:
  BootstrapServers: "broker1.yourdomain.local:9093,broker2.yourdomain.local:9093"
  SecurityProtocol: SaslSsl
  SaslMechanism: ScramSha512
  SaslUsername: your-kta-kafka-user
  SaslPassword: placeholder
  SslCaLocation: "C:\\Certificates\\KafkaClusterCA.pem"
  Acks: All

SchemaCache:
  RefreshIntervalMinutes: 5
```

---

# 13. Secret Management

The application loads:

```csharp
builder.Configuration.AddEnvironmentVariables();
```

Sensitive values are supplied using Windows Environment Variables.

Example:

```text
KafkaSettings__SaslPassword
```

Environment variables override YAML configuration at runtime.

This ensures deployment files never contain production secrets.

---

# 14. Error Handling

| Condition                 | Response                  |
| ------------------------- | ------------------------- |
| Invalid TLS certificate   | 403 Forbidden             |
| Unauthorized certificate  | 403 Forbidden             |
| Invalid request payload   | 400 Bad Request           |
| Schema validation failure | 422 Unprocessable Entity  |
| Schema unavailable        | 503 Service Unavailable   |
| Kafka unavailable         | 503 Service Unavailable   |
| Internal exception        | 500 Internal Server Error |

No invalid payload is ever published.

---

# 15. Logging & Observability

Structured logging should include:

* Correlation ID
* Client certificate identity
* Client IP
* Kafka topic
* Schema subject
* Schema version
* Processing duration
* Publish result
* Validation failures
* Exception details

Sensitive business data should never be written to logs.

---

# 16. Performance Characteristics

The middleware is optimised for low-latency publishing by:

* Caching parsed Avro schemas in memory
* Avoiding Schema Registry calls during request processing
* Reusing a singleton Kafka Producer
* Performing in-memory validation
* Refreshing schemas asynchronously in the background

Normal request processing requires no external dependencies other than Kafka.

---

# 17. Design Principles

* Single responsibility
* Secure by default
* Fail fast during startup
* Validate before publish
* Immutable schema cache during request processing
* Thread-safe operation
* Zero-downtime schema updates
* Configuration-driven deployment
* Enterprise-grade observability
* Least-privilege security model

---

# Technology Stack

| Category               | Technology                      |
| ---------------------- | ------------------------------- |
| Workflow Platform      | Tungsten TotalAgility (KTA)     |
| Middleware             | ASP.NET Core (.NET 8 / .NET 9)  |
| Hosting                | IIS 10                          |
| Operating System       | Windows Server 2022             |
| Messaging              | Apache Kafka                    |
| Serialization          | Apache Avro                     |
| Schema Registry        | Confluent Schema Registry       |
| Kafka Client           | Confluent.Kafka                 |
| Configuration          | YAML                            |
| Configuration Provider | NetEscapades.Configuration.Yaml |
| Authentication         | Mutual TLS (mTLS)               |
| Kafka Security         | SASL_SSL + SCRAM-SHA-512        |
| Application Identity   | ApplicationPoolIdentity         |

---

# Summary

This middleware provides a secure, high-performance integration layer between Tungsten TotalAgility and Apache Kafka. By preloading and caching Avro schemas, validating every message before publication, reusing a singleton Kafka producer, and enforcing multiple layers of authentication and authorization, the service minimizes latency while maintaining a strong enterprise security posture. Its fail-fast startup model, automatic schema refresh, configuration-driven deployment, and comprehensive observability make it suitable for long-term production use in regulated enterprise environments.

---

# Business Value & Strategic Benefits: Enterprise KTA to Kafka Integration Middleware

## Executive Summary

The proposed Integration Middleware establishes a secure, reusable, and enterprise-grade messaging platform between Tungsten TotalAgility (KTA) and Apache Kafka.

Rather than implementing bespoke integrations for individual workflows, the middleware introduces a centralized integration layer that standardizes security, validation, serialization, and event publishing across the organization.

This approach reduces development effort, lowers operational risk, improves security, and creates a scalable foundation for future event-driven integration initiatives.

---

# Business Objectives

The solution has been designed to achieve the following objectives:

* Standardize enterprise messaging
* Improve integration security
* Reduce operational complexity
* Eliminate duplicate development
* Support future business growth
* Increase system reliability
* Simplify ongoing maintenance
* Enable governed event-driven architecture

---

# Business Benefits

## Reusable Enterprise Service

Instead of creating a new Kafka integration for every workflow or application, the organisation maintains a single, centrally managed middleware service.

Benefits include:

* One codebase to maintain
* Consistent implementation standards
* Reduced support overhead
* Lower long-term maintenance costs
* Faster onboarding of new integrations

---

## Reduced Development Effort

Without the middleware, each KTA workflow would require custom Kafka integration logic.

With the middleware:

* Workflow developers only invoke a standard REST endpoint.
* The middleware manages schema resolution, validation, serialization, security, and publishing.

This significantly reduces implementation effort for future projects.

---

## Improved Security

The middleware introduces multiple layers of security that are centrally managed rather than individually implemented.

Security benefits include:

* Mutual TLS authentication
* Certificate-based authorization
* Internal network isolation
* Least-privilege Kafka access
* Secure secret management
* Enterprise audit logging

Centralizing these controls helps ensure that every integration follows the same security standards.

---

## Improved Data Quality

Every message is validated against the approved Apache Avro schema before publication.

This ensures:

* Required fields are present.
* Data types are correct.
* Invalid events are rejected before entering Kafka.

As a result, downstream systems receive consistent, governed, and predictable data.

---

## Reduced Operational Risk

The middleware is designed to fail safely.

It will not:

* Accept requests if required schemas cannot be loaded.
* Publish malformed messages.
* Continue operating with invalid configuration.

Fail-fast behaviour reduces the likelihood of silent failures and simplifies operational troubleshooting.

---

## Support for Schema Evolution

The middleware automatically refreshes cached schemas from the Schema Registry without requiring service restarts.

Benefits include:

* Reduced deployment windows
* Zero-downtime schema updates
* Faster rollout of new event versions
* Lower operational disruption

---

## High Performance

Performance is improved through:

* In-memory schema caching
* Singleton Kafka producer reuse
* Elimination of per-request Schema Registry lookups
* Lightweight request processing pipeline

These optimizations reduce latency while supporting higher throughput.

---

## Simplified Operations

Operational management is simplified through:

* Centralized configuration
* Environment-variable-based secret management
* Structured logging
* Health monitoring
* Consistent deployment model
* ApplicationPoolIdentity (no service account password management)

These features reduce the day-to-day administrative effort required to support the platform.

---

## Improved Governance

The middleware becomes the single approved pathway for publishing enterprise events from KTA.

This enables:

* Consistent security controls
* Standardized message validation
* Centralized auditing
* Controlled schema usage
* Simplified compliance reporting

Rather than governing many independent integrations, governance teams oversee one managed service.

---

# Strategic Benefits

The solution lays the groundwork for a broader event-driven architecture.

Future projects can leverage the same middleware to publish business events without implementing Kafka-specific logic, allowing application teams to focus on business functionality while the middleware manages messaging concerns.

This separation of responsibilities reduces technical debt and promotes architectural consistency across the enterprise.

---

# Cost Benefits

The middleware contributes to lower total cost of ownership by reducing:

* Duplicate development effort
* Integration maintenance
* Operational support requirements
* Security implementation effort
* Configuration inconsistencies
* Production defects caused by invalid event data

As additional KTA workflows are introduced, the marginal cost of integrating with Kafka decreases because the core capabilities are already provided by the shared service.

---

# Risk Reduction

The solution mitigates several common integration risks, including:

| Risk                            | Mitigation                               |
| ------------------------------- | ---------------------------------------- |
| Unauthorized system access      | Mutual TLS and certificate authorization |
| Invalid event data              | Avro schema validation                   |
| Schema incompatibility          | Central Schema Registry integration      |
| Credential exposure             | Environment-variable secret management   |
| Service account password expiry | ApplicationPoolIdentity                  |
| Kafka connectivity issues       | Centralized connection management        |
| Inconsistent implementations    | Single reusable middleware               |
| Operational outages             | Fail-fast startup and health monitoring  |

---

# Scalability

The middleware has been designed to scale alongside organisational demand.

As the number of workflows, business processes, and event types grows, no additional integration architecture is required.

New integrations typically involve:

1. Registering an Avro schema.
2. Updating configuration where necessary.
3. Calling the existing middleware endpoint.

This approach supports sustainable growth while minimizing additional development effort.

---

# Long-Term Value

This solution is not simply a connector between KTA and Kafka; it is a reusable enterprise integration capability.

By centralizing security, validation, serialization, and messaging, the organization gains a governed platform that can support future digital transformation initiatives, improve integration consistency, and reduce the ongoing cost and complexity of maintaining multiple point-to-point integrations.

The result is a secure, scalable, and maintainable integration service that delivers immediate value for the current project while establishing a foundation for future enterprise messaging requirements.
