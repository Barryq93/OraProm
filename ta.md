# Technical Design Blueprint: Reusable KTA to Kafka Integration Middleware

> **Target Environment:** Windows Server 2022 | .NET 8 / .NET 9  
> **Security Posture:** Mutual TLS (mTLS) Web Perimeter + SASL_SSL/SCRAM-SHA-512 Data Layer  
> **Data Governance:** Dynamic Confluent Schema Registry Resolution with Avro Serialization

---

# Overview

This middleware provides a reusable integration layer between **Tungsten TotalAgility (KTA)** and **Apache Kafka**, overcoming the limitations of the native marketplace connector.

Instead of connecting KTA directly to Kafka, the solution introduces a high-performance translation layer that:

- Accepts secure REST requests from KTA
- Performs mutual TLS authentication
- Dynamically resolves Avro schemas
- Serializes arbitrary JSON payloads into Avro
- Publishes securely to Kafka using SASL_SSL

The design is reusable across workflows, schemas, and Kafka topics without requiring application code changes.

---

# 1. Architecture

## Data Flow

```text
+-------------------------+
| Tungsten TotalAgility   |
| Workflow                |
+-----------+-------------+
            |
            | HTTPS (mTLS)
            |
            v
+-------------------------+
| ASP.NET Core Middleware |
| (.NET 8 / .NET 9)       |
+-----------+-------------+
            |
            | Validate Client Certificate
            |
            v
+-------------------------+
| Schema Registry         |
| Get Latest Avro Schema  |
+-----------+-------------+
            |
            | Serialize JSON -> Avro
            |
            v
+-------------------------+
| Kafka Producer          |
| SASL_SSL + SCRAM-SHA512 |
+-----------+-------------+
            |
            v
+-------------------------+
| Apache Kafka Cluster    |
+-------------------------+
```

---

## Processing Pipeline

### 1. Ingress

A **KTA Integration Activity** sends a unified REST payload to the middleware.

---

### 2. Authentication

The middleware:

- Terminates HTTPS
- Verifies the TLS handshake
- Validates the presented client certificate

---

### 3. Schema Resolution

The middleware extracts the requested:

```text
schemaSubject
```

It then:

- Connects to the Confluent Schema Registry
- Retrieves the latest schema version
- Stores it in an in-memory cache for reuse

---

### 4. Serialization

A generic mapping engine converts arbitrary JSON into an Avro `GenericRecord`.

Because schemas are dynamically loaded, no application recompilation is required when schemas evolve.

---

### 5. Egress

The middleware establishes a secure Kafka producer connection using:

- Native Kafka protocol
- SSL encryption
- SASL/SCRAM authentication

Serialized Avro messages are then published to Kafka.

---

# 2. Infrastructure

## Hosting

| Component | Value |
|-----------|-------|
| Operating System | Windows Server 2022 |
| Web Server | IIS 10 |
| Runtime | .NET 8 / .NET 9 |
| Hosting Bundle | ASP.NET Core Hosting Bundle |

---

## Application Pool

The IIS Application Pool runs as:

```text
ApplicationPoolIdentity
```

Advantages include:

- No stored service account passwords
- No Active Directory password rotation
- Minimal administrative maintenance
- Secure virtual Windows identity

---

## Network Authentication

When accessing SMB shares or internal resources, IIS authenticates as:

```text
DOMAIN\SERVER_NAME$
```

This allows administrators to grant permissions directly to the server's Active Directory computer account.

---

# 3. Security Perimeter

## Mutual TLS (mTLS)

Access is restricted exclusively to trusted KTA servers.

### IIS Configuration

```text
Require SSL
Client Certificates -> Require
```

Unauthenticated traffic is rejected before reaching the application.

---

## Application Authorization

Within ASP.NET Core:

```csharp
HttpContext.Connection.ClientCertificate
```

is inspected.

The middleware extracts:

```text
X509NameType.SimpleName
```

Example:

```text
kta-core-worker.yourdomain.local
```

The Common Name (CN) is compared against an internal whitelist.

Only matching certificates are permitted to continue.

---

# 4. Configuration Management

Configuration is externalized using YAML via:

```text
NetEscapades.Configuration.Yaml
```

Example:

```yaml
Logging:
  LogLevel:
    Default: Information
    Microsoft.AspNetCore: Warning

SecuritySettings:
  AllowedKtaClientCn: "kta-core-worker.yourdomain.local"

SchemaRegistrySettings:
  Url: "https://yourdomain.local"

KafkaSettings:
  BootstrapServers: "broker1.yourdomain.local:9093,broker2.yourdomain.local:9093"

  SecurityProtocol: SaslSsl
  SaslMechanism: ScramSha512

  SaslUsername: your-kta-kafka-user
  SaslPassword: YourStrongSecretPasswordHere

  SslCaLocation: "C:\\Certificates\\KafkaClusterCA.pem"

  Acks: All
```

---

# 5. Secret Management

The application enables environment variable overrides during startup:

```csharp
builder.Configuration.AddEnvironmentVariables();
```

Sensitive values should **not** remain inside configuration files.

Instead, production administrators inject secrets through Windows System Environment Variables.

Example:

```text
KafkaSettings__SaslPassword
```

Environment variables override YAML values at runtime.

Benefits include:

- No plaintext passwords in deployment artifacts
- Secure DevOps pipelines
- Easy secret rotation
- Separation of infrastructure and application configuration

---

# 6. Security Layers

| Layer | Technology |
|--------|------------|
| Transport Security | HTTPS + TLS |
| Client Authentication | Mutual TLS |
| Application Authorization | Certificate CN Whitelist |
| Kafka Authentication | SASL_SSL |
| SASL Mechanism | SCRAM-SHA-512 |
| Message Format | Apache Avro |
| Schema Governance | Confluent Schema Registry |

---

# 7. Design Benefits

- Reusable across multiple KTA workflows
- Dynamic schema resolution
- No code changes required for schema evolution
- Strong end-to-end encryption
- Mutual TLS authentication
- Secure Kafka authentication using SCRAM-SHA-512
- Zero service account password management
- Environment-variable-based secret management
- Production-ready deployment on IIS and Windows Server 2022

---

# Technology Stack

| Category | Technology |
|----------|------------|
| Workflow Platform | Tungsten TotalAgility (KTA) |
| Middleware | ASP.NET Core (.NET 8 / .NET 9) |
| Web Server | IIS 10 |
| Operating System | Windows Server 2022 |
| Serialization | Apache Avro |
| Schema Management | Confluent Schema Registry |
| Messaging Platform | Apache Kafka |
| Kafka Client | Confluent.Kafka |
| Configuration | YAML |
| Configuration Provider | NetEscapades.Configuration.Yaml |
| Authentication | Mutual TLS (mTLS) |
| Kafka Security | SASL_SSL + SCRAM-SHA-512 |
| Identity | ApplicationPoolIdentity |

---

# Summary

This architecture provides a secure, reusable, and enterprise-grade integration layer between **Tungsten TotalAgility** and **Apache Kafka**. By separating workflow execution from streaming infrastructure, the middleware enables dynamic schema resolution, secure message serialization, and resilient Kafka publishing while adhering to modern enterprise security and operational best practices.
