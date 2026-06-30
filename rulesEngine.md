You are an expert Ansible role author for RHEL-based systems.

I need you to design and implement a production-ready Ansible role to deploy and manage the OpenTelemetry Java agent on **RHEL 8** hosts. The role will set up a shared installation that multiple JVM-based applications (WebSphere and standalone Java processes) can use.

### High-level requirements

- Target OS: RHEL 8.
- Install base directory: `/apps/opentelemetry` (create if missing).
- Inside this base directory, create:
  - `/apps/opentelemetry/bin` for agent JARs and versioned binaries.
  - `/apps/opentelemetry/config` for Java properties configuration files.
- Implement a **symlink-based “current” layout** to make upgrades and rollbacks easy:
  - The actual agent JAR should be stored with a versioned filename, e.g.:
    - `/apps/opentelemetry/bin/opentelemetry-javaagent-<version>.jar`
  - There should be a stable symlink that does **not** contain the version, e.g.:
    - `/apps/opentelemetry/bin/opentelemetry-javaagent.jar` → `opentelemetry-javaagent-<version>.jar`
  - The role must manage this symlink so that switching the “current” version is trivial and safe.
- Permissions and ownership:
  - The installation should be readable and executable by multiple application owners on the same host.
  - Use a sensible owner/group model (for example, `root` owner and a shared group like `otel` or another configurable group).
  - Ensure the directories and agent JAR are world-readable and executable where appropriate so other users can attach the agent in their JVM args.
  - Avoid overly permissive settings (no `777`); choose secure defaults while still allowing multiple users to consume the agent and config files.
- Configuration files:
  - The role should provide the **Java properties files** for the OpenTelemetry Java agent (`otel.javaagent.configuration-file`).
  - Properties files should live under `/apps/opentelemetry/config`, for example:
    - `/apps/opentelemetry/config/<service_name>.properties`
  - Use Jinja2 templates so that properties can be parameterized per application/service.
  - The role should not hardcode service names; instead, accept variables to generate one or more properties files per host.
  - Each JVM process (WebSphere server, standalone JAR, etc.) will point to its own properties file via:
    - `-Dotel.javaagent.configuration-file=/apps/opentelemetry/config/<service_name>.properties`
- Integration pattern:
  - WebSphere JVMs and other Java processes will be configured separately (outside this role) to use:
    - `-javaagent:/apps/opentelemetry/bin/opentelemetry-javaagent.jar`
    - `-Dotel.service.name=<service_name>`
    - `-Dotel.javaagent.configuration-file=/apps/opentelemetry/config/<service_name>.properties`
  - The role’s responsibility is to ensure the agent JAR and properties files are correctly installed, versioned, and accessible; it does NOT need to modify WebSphere configs directly.
- Multiple properties files per server:
  - A single host may run multiple JVMs, each with its own service name and properties file.
  - The role should support deploying **multiple properties files per server** based on a list of applications/services defined via variables.
  - Each item could include at least:
    - `service_name`
    - `otel_properties` (a dict or map of key/value pairs to render into the properties file template).
- Idempotence and upgrades:
  - The role must be fully idempotent.
  - It should handle:
    - Initial install.
    - Upgrading the agent to a new version (drop new JAR, update symlink).
    - Leaving older versioned JARs in place for rollback if desired (configurable).
  - Symlink updates must not break running processes; assume JVMs will pick up the new agent only after restart.
- RHEL 8 best practices:
  - Use appropriate SELinux contexts if necessary (but only if they are required).
  - Ensure the role does not conflict with existing system packages or FHS conventions for third-party software.

### Ansible role structure

Please generate:

1. A clear **role structure**:
   - `tasks/main.yml`
   - `defaults/main.yml`
   - `vars/main.yml` (if needed)
   - `templates/otel-agent.properties.j2` (or similar)
   - `handlers/main.yml` (if needed)
   - `README.md` content describing how to use the role.
2. `defaults/main.yml`:
   - Provide sensible defaults, such as:
     - `otel_install_base: /apps/opentelemetry`
     - `otel_bin_dir: "{{ otel_install_base }}/bin"`
     - `otel_config_dir: "{{ otel_install_base }}/config"`
     - `otel_version: "2.28.1"` (example default, overridable).
     - `otel_owner: root`
     - `otel_group: otel`
     - `otel_dir_mode: "0755"`
     - `otel_file_mode: "0644"`
     - `otel_services: []` (list of per-service configs).
3. `tasks/main.yml`:
   - Create the base, `bin`, and `config` directories with correct ownership and permissions.
   - Install or copy the versioned agent JAR into `bin`.
   - Create or update the symlink `opentelemetry-javaagent.jar` pointing to the current versioned JAR.
   - Render properties files under `config` based on `otel_services` list.
   - Use `loop` and `with_items` to handle multiple services.
4. `templates/otel-agent.properties.j2`:
   - Render each `otel_properties` map into `key=value` lines.
   - Include comments at the top indicating the service name and that the file is managed by Ansible.
5. A brief `README.md` text explaining:
   - How to include the role.
   - How to set `otel_services` for multiple JVMs.
   - How to override the agent version and base directory.
   - How the symlink-based upgrade/rollback works.

Make sure all YAML examples are syntactically correct, and follow Ansible best practices for RHEL 8. Use clear variable names and add brief inline comments where it improves readability.
