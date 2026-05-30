Act as a senior enterprise solution architect, Java Spring Boot architect, and DMN/rules-engine specialist.

You are tasked with producing a complete, enterprise-grade High-Level Design (HLD) and Low-Level Design (LLD) for a Rules Execution Microservice. This microservice will manage and execute business decision rules defined as DMN Decision Tables and will be used by multiple domains (e.g., payments, credit, lending) in a regulated banking environment.

## Context and Constraints

The service must:
- Be built using Java 21 + Spring Boot 3.x.
- Expose REST APIs (read/execute only).
- Use Operaton as the DMN/rules execution engine in embedded mode (library dependency, not a standalone server).
- Store rules in an Oracle 19c+ database.
- Store DMN/XML rule definitions as CLOB in the database.
- Support versioning: each logical rule has a rule_id; each rule_id may have multiple versions.
- Enforce that only one version of a given rule_id can be active at a time.
- Include is_active flags on rule versions.
- Cache active rules in a distributed cache.
- Use Hazelcast as the distributed cache, deployed as an embedded cluster.
- Run in an active-active deployment with 2 VM nodes, each running a rules-svc.jar with an embedded Hazelcast member.
- Use Ping Access for authentication and authorisation; the service itself does not issue tokens.
- Register with Eureka for service discovery.
- Spring Boot Admin will handle cache clearing via standard Spring Boot actuator endpoints; no custom admin API for cache management is required.
- Rule inserts/updates/activations are performed manually by DBA via direct SQL; the service does not expose CRUD or activation/deactivation endpoints.
- Publish rule execution events to a Kafka topic for audit/observability.
- Integrate with Prometheus, Splunk, and OpenTelemetry (OTel) for metrics, logging, and tracing.
- Support DMN Decision Tables only (no DRDs, no complex FEEL beyond standard table predicates).
- Implement idempotency via a client-supplied X-Idempotency-Key header, enforced using Hazelcast.
- Use Operaton to infer required input variables from the DMN XML and validate the request payload at runtime.
- Return HTTP 404 when no active rule is found or when no DMN rule row matches.
- Be designed for multi-domain reuse: payments, credit, lending, etc. Each domain must be able to own its own rules independently while sharing the same runtime platform.

Required endpoints (read/execute only):
- GET /v1/rules — return a list of available active rules.
- GET /v1/rules/{ruleId} — return details for a specific rule, including version history and input/output metadata.
- POST /v1/rules/{ruleId}/execute — validate request payload and execute the active version of the rule.

Design principles:
- Prefer enterprise-grade, maintainable, clear design.
- Avoid exposing internal rule logic unnecessarily.
- Assume multiple consuming systems and multiple business domains.
- Assume audit and traceability are critical.
- Keep the service loosely coupled from Operaton by introducing an internal RuleEnginePort interface.
- Make the design suitable for future migration to another rules engine if required.
- Ensure the design supports multi-domain reuse with clear ownership boundaries.
- No admin API for rule lifecycle or cache management; cache clearing is handled via Spring Boot Admin actuator endpoints, and rule lifecycle is handled manually by DBA.

## Required Output

Produce a single, comprehensive design document that includes the following sections:

1. Executive Summary
   - One-paragraph overview of the service, its purpose, and key design choices.
   - Explicit statement that the service is a reusable, multi-domain platform.
   - Explicit statement that rule lifecycle is manual (DBA) and cache clearing is via Spring Boot Admin, not custom admin API.

2. Assumptions and Constraints
   - A table of explicit assumptions (e.g., embedded Operaton, two active-active nodes, Oracle 19c, Ping Access auth, manual DBA rule inserts, Spring Boot Admin + Eureka for cache handling, etc.).
   - For each assumption, note the impact if it turns out to be wrong.

3. Architecture Overview
   - Logical architecture with 5 layers: API, Validation, Rule Execution, Engine Abstraction (RuleEnginePort), Infrastructure.
   - A Mermaid component diagram showing:
     - Consumers (banking apps)
     - Ping Access gateway
     - Node 1 / Node 2 (active-active Spring Boot apps)
     - Embedded Operaton DMN engine
     - Hazelcast cluster (rule cache + idempotency cache)
     - Oracle DB (rules + audit)
     - Kafka topic
     - OTel Collector → Prometheus / Splunk
     - Eureka (service discovery)
     - Spring Boot Admin (cache clearing via actuator)

4. High-Level Design (HLD)
   For each sub-section, be explicit and detailed:
   - Logical architecture description.
   - Component diagram (Mermaid).
   - API overview table with method, path, auth role, description (read/execute only endpoints).
   - Data flow for rule execution (step-by-step).
   - Caching strategy:
     - Hazelcast maps: active-rules and idempotency-keys.
     - TTL, eviction policy, backup-count, active-active consistency.
     - Guidance on capacity (e.g., 300–800 rules with 500 MB heap, and how it scales).
     - Cache clearing via Spring Boot Admin actuator endpoints (no custom admin API).
   - Database strategy:
     - Two schemas: RULES_OWNER and AUDIT_OWNER.
     - Function-based unique index to enforce single active version per rule_id.
     - CLOB storage for DMN XML.
     - Optional derived metadata columns: DMN_XML_SIZE_BYTES and DMN_HASH.
     - Optional trigger for size/hash maintenance (describe pros/cons).
     - Rule lifecycle is manual via DBA SQL; service is read-only for rules.
   - Security model:
     - Authentication via Ping Access.
     - Authorization via JWT roles.
     - Transport, secrets, input sanitisation, audit, and rule logic confidentiality.
   - Observability model:
     - Prometheus metrics (list key metrics).
     - OpenTelemetry tracing (spans, correlation ID propagation).
     - Splunk logging (MDC fields, JSON logs).
     - Kafka audit events schema.
   - Error handling approach:
     - HTTP status codes, error codes, and standard error envelope.
   - Deployment view:
     - ASCII diagram of the data centre layout: Ping Access, two app nodes, Eureka, Spring Boot Admin, Oracle RAC, Kafka, OTel Collector.
     - JVM flags and configuration.

5. Low-Level Design (LLD)
   For each sub-section, be explicit and detailed:
   - Package structure for a Spring Boot 3.x application.
   - Controller design:
     - RulesController only (no AdminController).
   - Service design:
     - RuleEnginePort interface (engine abstraction).
     - OperatonRuleEngine implementation sketch.
     - RuleService orchestration flow.
     - IdempotencyService using Hazelcast.
     - RuleCacheService.
     - AuditService.
   - DTOs / request/response models:
     - RuleExecuteRequest, RuleExecuteResponse, RuleListResponse, RuleDetailResponse, ErrorResponse.
     - Sample JSON for each.
   - Oracle table design:
     - RULE_DEFINITIONS and RULE_VERSIONS tables with full DDL.
     - RULE_EXECUTION_AUDIT table with full DDL.
     - Function-based unique index to enforce single active version.
     - Optional trigger for DMN_XML_SIZE_BYTES and DMN_HASH.
   - Active-rule uniqueness constraint:
     - DB-level enforcement via function-based unique index.
     - Rule lifecycle handled manually by DBA (no activation/deactivation service flow).
   - Rule execution sequence (Mermaid sequence diagram).
   - Cache design:
     - hazelcast.xml configuration.
     - Explanation of backup-count, eviction, TTL, and active-active consistency.
     - Cache clearing via Spring Boot Admin actuator (e.g., /actuator/cache/active-rules/clear).
   - Engine abstraction and DMN execution flow:
     - How Operaton parses DMN, validates inputs, and executes decision tables.
     - How the engine abstraction supports future migration.
   - Input validation approach:
     - Layer 1: Bean Validation.
     - Layer 2: DMN-inferred validation via Operaton.
   - Exception handling:
     - Custom exceptions and @RestControllerAdvice sketch.
   - Logging and audit model:
     - MDC fields.
     - Kafka event schema.
     - Audit table usage.
   - Unit and integration test strategy:
     - Key unit tests and integration tests.
     - Use of Testcontainers for Oracle, Hazelcast, and Kafka.

6. Mermaid Diagrams
   Include complete, valid Mermaid diagrams for:
   - Component architecture (graph TB).
   - Rule execution sequence (sequenceDiagram).
   - Database entity relationship (erDiagram).

7. API Examples with Sample JSON
   - Example request and response for:
     - GET /v1/rules
     - GET /v1/rules/{ruleId}
     - POST /v1/rules/{ruleId}/execute
   - Include sample JSON payloads and responses, including error responses.

8. Risks, Trade-offs, and Recommendations
   - A table with:
     - Risk description.
     - Severity (Low/Medium/High).
     - Recommendation.
   - Include at least risks for: embedded Operaton, Hazelcast split-brain, CLOB performance, Kafka unavailability, PII in audit CLOB, DMN engine migration, manual rule lifecycle (DBA).

9. Open Questions to Confirm Before Implementation
   - A list of critical open questions, including:
     - PII masking policy.
     - Idempotency key TTL.
     - Kafka topic configuration.
     - Oracle CLOB storage and tablespace quotas.
     - Ping Access JWT claim structure for roles.
     - Spring Boot Admin actuator endpoint path for cache clearing.
     - Hazelcast network configuration.
     - OTel Collector endpoint and backend.
     - DMN validation gate before DBA deploy.
     - Service registration and health check strategy with Eureka.

10. Multi-Domain Design Considerations
    - Explicitly address how the service supports multiple domains (payments, credit, lending, etc.).
    - Describe:
      - domain_code on rules and versions.
      - Domain-scoped endpoints (e.g., /v1/domains/{domain}/rules) if needed.
      - Domain ownership boundaries and authorization.
      - Domain-specific metrics and dashboards.
      - How rule namespaces are isolated per domain but the runtime platform is shared.

## Format Requirements

- Use clear, enterprise-grade language.
- Use Markdown with headings, tables, code blocks, and Mermaid diagrams.
- All code snippets must be syntactically valid Java or SQL.
- All Mermaid diagrams must be valid and render correctly.
- Keep the document self-contained and implementation-ready.
- Do not ask clarifying questions; make reasonable assumptions where needed and document them clearly.

Produce the full design document now.