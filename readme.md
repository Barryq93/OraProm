---

# OraProm: Oracle to Prometheus Exporter

OraProm is a Python-based application that exports Oracle database metrics to Prometheus. It periodically queries an Oracle database, extracts metrics, and exposes them in a format that Prometheus can scrape. This tool is inspired by [db2Prom](https://github.com/Barryq93/db2Prom), a similar tool for exporting DB2 metrics to Prometheus.

---

## Table of Contents
1. [Features](#features)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Configuration](#configuration)
5. [Running the Application](#running-the-application)
6. [Building an Executable with PyInstaller](#building-an-executable-with-pyinstaller)
7. [Testing](#testing)
8. [Contributing](#contributing)
9. [License](#license)

---

## Features
- **Oracle Database Integration**: Connects to Oracle databases and executes custom SQL queries.
- **Prometheus Metrics**: Exports query results as Prometheus gauges.
- **Configurable Queries**: Define SQL queries and metrics in a YAML configuration file.
- **Asynchronous Execution**: Uses `asyncio` for efficient query execution and connection management.
- **Logging**: Logs application activity and errors to files.

---

## Prerequisites
- Python 3.7 or higher.
- Oracle client libraries (`oracledb`).
- Prometheus (to scrape the exposed metrics).

---

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/oraprom.git
   cd oraprom
   ```

2. Install the required Python packages:
   ```bash
   pip install -r requirements.txt
   ```

3. Install Oracle client libraries:
   - Follow the [oracledb installation guide](https://python-oracledb.readthedocs.io/en/latest/user_guide/installation.html) to set up the Oracle client.

---

## Configuration
The application is configured using a `config.yaml` file. Here's an example configuration:

```yaml
global_config:
  log_level: INFO
  retry_conn_interval: 60
  default_time_interval: 15
  log_path: "logs/"
  port: 9844

queries:
  - name: "Locations"
    runs_on: ["production"]
    time_interval: 10
    query: |
      SELECT COUNT(*) FROM countries
    gauges:
      - name: "location_count"
        desc: "Count of locations"
        col: 1

connections:
  - db_host: "192.168.50.72"
    db_name: "FREEPDB1"
    db_port: 1521
    db_user: "my_user"
    db_passwd: "password"
    tags: [production, proddb1]
    extra_labels:
      dbinstance: HR
      dbenv: production
```

### Configuration Fields
- **`global_config`**:
  - `log_level`: Logging level (e.g., INFO, DEBUG).
  - `retry_conn_interval`: Interval (in seconds) to retry Oracle connection.
  - `default_time_interval`: Default interval (in seconds) for query execution.
  - `log_path`: Directory to store log files.
  - `port`: Port for the Prometheus HTTP server.

- **`queries`**:
  - `name`: Name of the query.
  - `query`: SQL query to execute.
  - `gauges`: List of Prometheus gauges to create from the query results.
    - `name`: Name of the gauge.
    - `desc`: Description of the gauge.
    - `col`: Column index in the query result to use as the gauge value.

- **`connections`**:
  - `db_host`: Oracle database host.
  - `db_name`: Oracle database name.
  - `db_port`: Oracle database port.
  - `db_user`: Oracle database user.
  - `db_passwd`: Oracle database password.
  - `extra_labels`: Additional labels to attach to Prometheus metrics.

---

## Running the Application
1. Ensure the `config.yaml` file is properly configured.
2. Run the application:
   ```bash
   python app.py config.yaml
   ```
3. The Prometheus metrics will be available at `http://localhost:9844/metrics`.

---

## Building an Executable with PyInstaller
To create a standalone executable for easier deployment:

1. Install PyInstaller:
   ```bash
   pip install pyinstaller
   ```

2. Build the executable:
   ```bash
   pyinstaller --onefile app.py
   ```

3. The executable will be located in the `dist` directory. You can run it like this:
   ```bash
   ./dist/app config.yaml
   ```

---

## Testing
The project includes unit tests to ensure the functionality of the application. To run the tests:

1. Install `pytest` and `pytest-asyncio`:
   ```bash
   pip install pytest pytest-asyncio
   ```

2. Run the tests:
   ```bash
   pytest tests/ -v
   ```

---

## Contributing
Contributions are welcome! If you'd like to contribute, please follow these steps:
1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Submit a pull request with a detailed description of your changes.

---

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

## Acknowledgments
- Inspired by [db2Prom](https://github.com/Barryq93/db2Prom).
- Uses the [oracledb](https://python-oracledb.readthedocs.io/) library for Oracle database connectivity.
- Built with [Prometheus Client](https://github.com/prometheus/client_python) for metric export.

---