Create a complete Ansible role for managing online IBM Db2 11.5.9 database maintenance in an IBM Content Manager v8 (CM8) environment. 

The role must handle state tracking using a custom support schema inside the database to allow multi-day, interrupted, or window-based execution without data loss.

### Scope & Requirements for the AI Code Generation

1. **Database Support Schema & Tables (`DBA_MAINTENANCE`):**
   - Create a DDL initialization script or template task that sets up a dedicated support schema (e.g., `DBA_MAINTENANCE`).
   - Include a **`TASK_QUEUE`** table to track individual target tables, prioritizing them, and holding columns for lifecycle statuses: `PENDING`, `IN_PROGRESS`, `PAUSED`, and `COMPLETED` for table reorgs, index reorgs, and runstats.
   - Include an **`EXECUTION_LOG`** table for auditing start times, pauses, resumptions, completions, and errors with timestamps.

2. **Ansible Role Structure & Best Practices:**
   - Follow standard Ansible role directory structure (`defaults`, `tasks`, `handlers`, `templates`, `meta`).
   - Use proper Db2 connection context handling and leverage `become_user` for the Db2 instance owner (e.g., `db2inst1`).
   - Parameterize connection variables like database name, instance user, and support schema name.

3. **Maintenance Workflow (Idempotent & Resumable):**
   - **Discovery Task:** Query the `TASK_QUEUE` table to fetch the highest-priority table that is `PENDING` or `PAUSED`. Gracefully exit if no tasks remain.
   - **Table Reorg (Inplace):** 
     - If the status is `PENDING`, initiate `REORG TABLE schema.tablename INPLACE`.
     - If the status is `PAUSED`, invoke `REORG TABLE schema.tablename INPLACE RESUME`.
     - *Constraint handling:* Include logic/variables to support pausing execution cleanly if maintenance windows close or if a backup routine triggers.
   - **Index Reorg:** Once table reorg reaches completion, execute online index reconstruction using `REORG INDEXES ALL FOR TABLE schema.tablename ALLOW WRITE ACCESS`.
   - **Statistics Collection (`RUNSTATS`):** Execute online statistics gathering using `RUNSTATS ON TABLE schema.tablename USE PROFILE ALLOW WRITE ACCESS` to ensure the CM8 query optimizer stays tuned without locking the application out.
   - **Audit & State Updates:** Update the `TASK_QUEUE` and `EXECUTION_LOG` tables dynamically throughout each transition so the playbook state persists across runs.

4. **Error Handling & Robustness:**
   - Implement `failed_when` conditions to catch actual Db2 SQL errors vs. expected return codes.
   - Ensure rollback or status-flipping to `PAUSED` or `FAILED` if a command errors out, preventing infinite failure loops.
