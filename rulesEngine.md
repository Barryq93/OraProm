Create a complete Ansible role for managing online IBM Db2 11.5.9 database maintenance in an IBM Content Manager v8 (CM8) environment. 

The role must handle state tracking using a pre-existing support schema inside the database (assumed to be set up out-of-band and documented as a prerequisite) to allow multi-day, interrupted, or window-based execution without data loss. The playbook will be executed directly as the Db2 instance owner (e.g., db2inst1).

### Scope & Requirements for the AI Code Generation

1. **Prerequisites & Documentation Context:**
   - Assume the `DBA_MAINTENANCE` support schema, `TASK_QUEUE` table, and `EXECUTION_LOG` table are created out-of-band. Document the exact DDL structure and setup steps required for these objects as part of the role's README or a prerequisite file.
   - The role will rely on querying and updating these pre-existing database tables for state management.

2. **Ansible Role Structure & Best Practices:**
   - Follow standard Ansible role directory structure (`defaults`, `tasks`, `handlers`, `templates`, `meta`, `README.md`).
   - Structure tasks assuming execution is performed under the Db2 instance owner user context (`db2inst1`), handling Db2 CLI environment source commands (`. ~db2inst1/sqllib/db2profile`) if required within the shell/command tasks.
   - Parameterize connection variables like database name, instance user, and support schema name.

3. **Maintenance Workflow, Scheduling Intelligence & Dual Logging:**
   - **Frequency & REORGCHK Prioritization:** Incorporate logic where every table must be reorganized at least once every 30 days (tracked via `last_run_timestamp` in the task queue). Additionally, pre-run evaluations or `REORGCHK` analysis should dynamically flag tables or indexes that breach structural thresholds, bumping their priority to run sooner.
   - **Backup Detection & Safety:** Prior to executing or continuing reorg tasks, check for active online database backups (e.g., querying Db2 monitoring views/snapshots or checking backup process status). If an active backup is detected, gracefully issue a `REORG TABLE schema.tablename INPLACE PAUSE` command, update state tracking, log the event, and wait/skip safely.
   - **Discovery Task:** Query the pre-existing `TASK_QUEUE` table to fetch the highest-priority table that is `PENDING`, `PAUSED`, past the 30-day window, or flagged by `REORGCHK`. Gracefully exit if no tasks remain.
   - **Table Reorg (Inplace):** 
     - If the status is `PENDING`, initiate `REORG TABLE schema.tablename INPLACE`.
     - If the status is `PAUSED`, invoke `REORG TABLE schema.tablename INPLACE RESUME`.
   - **Index Reorg:** Once table reorg reaches completion, execute online index reconstruction using `REORG INDEXES ALL FOR TABLE schema.tablename ALLOW WRITE ACCESS`.
   - **Statistics Collection (`RUNSTATS`):** Execute online statistics gathering using `RUNSTATS ON TABLE schema.tablename USE PROFILE ALLOW WRITE ACCESS` to ensure the CM8 query optimizer stays tuned without locking the application out.
   - **Dual Logging Mechanism:** Ensure all status updates, milestones, errors, and events are logged **both** to the database `EXECUTION_LOG` table and to a local filesystem log file on the host.

4. **Error Handling & Robustness:**
   - Implement `failed_when` conditions to catch actual Db2 SQL errors vs. expected return codes.
   - Ensure rollback or status-flipping to `PAUSED` or `FAILED` if a command errors out, preventing infinite failure loops.
