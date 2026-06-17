# High-Level Design Ask: Internal Document Management Replacement

## Objective
Design a high-level architecture for a new internal document management application to replace the current IBM Content Navigator solution, including custom plugins and EDS logic, with a simpler, modern, and maintainable Spring Boot-based application.

## Business Context
The current solution is based on IBM Content Navigator with custom plugins and External Data Services. The target state is a cleaner internal application focused on internal users, reduced architectural complexity, improved maintainability, and faster delivery.

The organization already has key enterprise capabilities in place:
- Spring Boot backend foundation
- Existing document store and retrieval APIs
- Existing customer lookup API
- Active Directory / SSO authentication available or planned
- Email-based virus scanning already performed before documents are received

Because the team owns the document storage and retrieval APIs, the replacement can be significantly simpler than a generic ECM front end.

## Core User Workflow
The new application should support the following user journey:
1. Authenticate using Active Directory / SSO.
2. Search for customers using the existing customer API.
3. Select a customer from search results.
4. Search for documents associated with the selected customer.
5. View a document list with metadata.
6. Retrieve and view documents.
7. Upload documents with metadata.

## Key Functional Expectations
### Search and retrieval
- Customer search must use the existing customer API.
- Document search must use the existing retrieval APIs.
- Users must be able to search for both customers and documents.
- Searches and access events must be auditable.

### Document viewing
- PDF documents should be viewable inline.
- Images should be viewable inline.
- Office documents can initially be downloaded, with optional future conversion-to-PDF support.
- Annotation capability is not required.
- There is no need to replicate IBM Daeja ViewONE feature-for-feature.

### Upload and metadata
- Users must be able to upload documents with metadata.
- Metadata entry should support dropdowns and text fields.
- Metadata and search behaviour should be driven by configurable business rules rather than hard-coded controller logic where practical.

## UI Direction
The preferred UI approach is server-side rendering with Spring Boot and Thymeleaf.

The design should confirm whether Thymeleaf is the right choice and evaluate only lightweight enhancements where they clearly improve usability. The current preference is to avoid a heavy SPA framework unless there is a strong justification.

A lightweight progressive enhancement approach such as Thymeleaf with HTMX is acceptable if it improves user experience for fragment refresh, dependent metadata fields, and dynamic search interactions without introducing SPA complexity.

## Security and Access Model
The application is internal-only and must integrate with Active Directory / SSO.

Access will be role/group-driven, with multiple business areas and different AD groups controlling read/write behaviour. Example areas discussed include:
- `/cib` with multiple AD groups, including separate read and write access
- `/gpvu` with separate associated groups
- `/admin` with a dedicated admin group

The design should determine the best structure for this, with the current architectural direction favoring:
- a shared `/docs` application area for standard document operations
- a separate `/admin` area for administrative capabilities
- group-aware behaviour driven by security context, configuration, and rules rather than duplicated endpoints per business area

The solution must avoid exposing customer IDs or document IDs in URLs where that could create insecure direct object reference risks. Session-scoped context and server-side authorization checks should be considered in the design.

## Session and Deployment Model
The application is expected to run as a clustered Spring Boot service behind a VIP on port 443.

The high-level design should include:
- clustered Spring Boot deployment behind a load balancer or VIP
- HTTPS exposed externally on port 443
- internal application nodes running on an internal port such as 8080
- forwarded header handling for correct redirect and SSO behaviour
- shared session persistence using a database-backed session store

An Oracle-backed session table for active user sessions, with expiry and cleanup after a configurable number of hours, is an accepted direction.

## Audit Requirements
Audit logging is required for at least the following events:
- who searched for which customer
- who searched for which document
- who viewed which document
- who uploaded which document
- relevant timestamps, user identity, and access context

The high-level design should include an audit logging approach and an admin-facing audit view.

## Workflow and Rules
### BPMN workflow
Operaton is required for the admin workflow.

The high-level design should identify where BPMN workflows are appropriate, especially for admin and approval-style processes, while avoiding unnecessary use of workflow for simple request/response document operations.

### DMN usage
There is interest in using Operaton's DMN engine for rules such as:
- search template selection
- upload template selection
- metadata field visibility
- required vs optional field behaviour
- dropdown selection logic
- approval-routing decisions

A key architectural idea is that business analysts may own and maintain DMN definitions.

The design should therefore evaluate:
- storing DMN artifacts outside the application JAR
- keeping DMN artifacts in a database-backed repository with versioning metadata
- deploying approved DMN definitions into Operaton for execution
- providing an admin UI for managing DMN definitions
- potentially embedding a browser-based DMN editor in the admin area rather than relying on packaged resources

The design should distinguish clearly between:
- DMN for decision logic
- Spring services and configuration/API sources for dynamic reference data and dropdown values
- security enforcement in Spring Security and service-layer authorization

## Content Navigator Replacement Considerations
The high-level design should explicitly address how the new solution replaces these IBM Content Navigator concepts:
- plugin architecture
- EDS-driven metadata logic
- repository browsing and document retrieval UX
- business-area-specific templates and behaviours

The design should also confirm whether there is any strategic reason to continue investing in Content Navigator rather than replacing it. Based on the current discussion, continuing on Content Navigator appears to offer little value unless there are hidden viewer or workflow dependencies not yet considered.

## First Deliverable Requested
The immediate next step is not a detailed low-level design or implementation plan.

The first deliverable requested is a **high-level design** that covers:
- target architecture
- main application modules
- security model
- UI approach
- integration approach
- session and deployment model
- audit model
- workflow and DMN positioning
- replacement strategy for IBM Content Navigator and EDS
- major risks, constraints, and design decisions

## Outcome Sought
Produce a pragmatic high-level design for an internal Spring Boot-based document management application that:
- replaces IBM Content Navigator and EDS with a simpler internal solution
- uses Thymeleaf-first server-side rendering
- integrates with existing enterprise APIs
- supports AD/SSO and group-based access
- supports auditing and admin workflow via Operaton
- uses DMN selectively for configurable business rules
- minimizes unnecessary infrastructure and architectural complexity
