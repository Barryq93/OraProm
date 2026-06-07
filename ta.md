# Tungsten TotalAgility (TTA) OOTB Training Plan: Email Triage & Document Processing

**Goal:** Equip a graduate engineer to independently configure an end-to-end TotalAgility pipeline that ingests emails, classifies documents, extracts data, accesses external databases via stored procedures, and exports payloads to a Spring Boot REST API while ensuring standard observability.

## Week 1: Platform Architecture & Workflow Basics
**Weekly Goal:** Understand the Tungsten TotalAgility (TTA) architecture and build a foundational routing workflow without document extraction.

* **Day 1: TTA Ecosystem & Navigation**
  * **Goal:** Understand the separation of concerns.
  * **Tasks:** Differentiate between the TTA Designer (development) and the TTA Workspace (end-user operations). Learn the concepts of Processes, Cases, and Jobs.
* **Day 2: Basic Process Orchestration**
  * **Goal:** Build a "Hello World" workflow.
  * **Tasks:** Use the TTA Designer canvas to drag and drop standard nodes (Start, Activity, End). Map basic string and integer variables between steps.
* **Day 3: Deterministic Routing & Business Rules**
  * **Goal:** Introduce logic and branching.
  * **Tasks:** Configure Decision nodes and Business Rules to route cases based on strict metadata conditions (e.g., routing to an "Urgent" path if a priority flag is true).
* **Day 4: Human-in-the-Loop Operations**
  * **Goal:** Configure manual fallback queues.
  * **Tasks:** Create a manual Activity node. Set up a basic Work Queue in the TTA Workspace so the grad can visually see how a human operator picks up and completes a routed task.
* **Day 5: Week 1 Capstone**
  * **Goal:** Deploy a functional routing process.
  * **Tasks:** Build a process that ingests a dummy payload, evaluates a business rule, routes to a manual queue, and completes successfully.

## Week 2: Email Triage & Classification
**Weekly Goal:** Configure the Message Connector for passive email polling and set up deterministic document classification.

* **Day 6: Message Connector Configuration**
  * **Goal:** Establish the email ingestion pipeline.
  * **Tasks:** Access the standalone Message Connector Configuration tool and set up a polled IMAP/Exchange mailbox to passively ingest emails and detach their payloads [1].
* **Day 7: Import Connections in TTA Designer**
  * **Goal:** Link the inbox to the workflow.
  * **Tasks:** Map the Message Connector to the TTA Designer by configuring an Import Connection that triggers a specific process whenever a new email arrives [2].
* **Day 8: Transformation Designer Basics**
  * **Goal:** Set up the classification environment.
  * **Tasks:** Open Transformation Designer, create a new Project, establish a basic document class hierarchy (e.g., "Invoices", "Support Tickets"), and upload a batch of sample PDFs [3].
* **Day 9: OOTB Classification**
  * **Goal:** Categorize documents using deterministic rules.
  * **Tasks:** Implement Layout Classification (matching visual structure) and Keyword Classification (triggering off specific anchor words like "Invoice" or "Complaint").
* **Day 10: Week 2 Capstone**
  * **Goal:** End-to-end ingestion and triage.
  * **Tasks:** Send test emails with mixed attachments. Validate that the Message Connector polls them and the Transformation Designer accurately classifies them into the correct queues based purely on keywords and layout.

## Week 3: OOTB Data Extraction & Validation
**Weekly Goal:** Extract structured data using standard locators and build validation forms for human exception handling.

* **Day 11: Standard Extraction Locators**
  * **Goal:** Extract text using regex and exact matches.
  * **Tasks:** Configure the Format Locator to extract standard patterns (e.g., regex for dates, currency, or UUIDs). Set up the Database Locator to match extracted text against an imported static dataset.
* **Day 12: Advanced Zone Locators**
  * **Goal:** Process highly structured, fixed-form documents.
  * **Tasks:** Use the Advanced Zone Locator to draw specific extraction zones on standard templates and apply basic image cleanup and Optical Character Recognition (OCR) profiles [4].
* **Day 13: Formatting & Validation Rules**
  * **Goal:** Ensure data hygiene before export.
  * **Tasks:** Apply formatters to raw extractions (e.g., stripping currency symbols or standardizing date formats). Set up hard validation rules that flag a document if a required field is missing.
* **Day 14: Generating Validation Forms**
  * **Goal:** Build the UI for data correction.
  * **Tasks:** Use the TTA Designer Form Builder to generate a Validation Form mapped to the document fields. Learn how to display the extracted data side-by-side with the document image.
* **Day 15: Week 3 Capstone**
  * **Goal:** End-to-end extraction and review.
  * **Tasks:** Process a batch of documents. Verify that OOTB locators extract the data perfectly. Intentionally process a faulty document to trigger a validation error, then correct it manually in the Workspace.

## Week 4: External Integrations & Observability
**Weekly Goal:** Interface with databases via stored procedures, export via REST API, and establish baseline observability.

* **Day 16: Database Integration & Stored Procedures**
  * **Goal:** Query external databases natively in TTA.
  * **Tasks:** Configure a Data Access activity node in the TTA Designer. Connect it to an external SQL database (like DB2) and execute a Stored Procedure, mapping the returned data set to process variables for enrichment [5].
* **Day 17: Configuring REST Integrations**
  * **Goal:** Establish the outbound connection to the backend.
  * **Tasks:** Set up a REST Web Service integration. Define the endpoint URL for the Spring Boot application, configure the HTTP POST method, map the validated JSON payload, and handle authentication headers.
* **Day 18: Error Handling & System Exception Logic**
  * **Goal:** Build resiliency into the export phase.
  * **Tasks:** Configure exception routing on the Web Service and Data Access nodes. If a database query fails or the Spring Boot API returns a 500 error, route the job to a delayed retry loop or a manual administrator exception queue [6].
* **Day 19: Observability and Performance Monitoring**
  * **Goal:** Expose TTA telemetry for external monitoring.
  * **Tasks:** Explore TTA's standard application logging (Event Viewer/SQL tables). While standard TTA does not have native OpenTelemetry, learn how to push custom log entries and system health metrics to third-party APM/observability platforms (like Grafana Alloy or Reveille) to track queue levels, SLA breaches, and node failures [7].
* **Day 20: Final Program Capstone**
  * **Goal:** Complete end-to-end system test.
  * **Tasks:** Send an email with a document. Monitor the system as it ingests, classifies, extracts, executes a database lookup via stored procedure, validates, and exports via REST. Check external observability dashboards to confirm the process executed within SLA and the payload was delivered.

---

## References

| Ref | Topic | Source Documentation / Features |
| :--- | :--- | :--- |
| [1] | Email Ingestion | Configure Message Connector |
| [2] | Process Triggers | Import Settings |
| [3] | Document Classification | Use Transformation Designer |
| [4] | Data Extraction | Advanced Zone Locator |
| [5] | Database Connectivity | Configure a Data Access Activity (Stored Procedures) [web:39] |
| [6] | Exception Handling | Generic Exception Handler / System Exceptions [web:41] |
| [7] | Observability | Application Monitoring and Telemetry Integration [web:43] |