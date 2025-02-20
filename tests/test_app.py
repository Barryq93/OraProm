import pytest
from unittest.mock import patch, MagicMock
import asyncio
import app
import sys

def test_setup_logging(tmpdir):
    """Test the setup_logging function."""
    log_path = tmpdir.mkdir("logs")
    log_level = "INFO"

    app.setup_logging(str(log_path), log_level)

    # Check if log files are created
    assert log_path.join("oraProm.log").exists()
    assert log_path.join("oraProm.err").exists()

def test_oracle_instance_connection(mock_config):
    """Test the oracle_instance_connection function."""
    config_connection = mock_config["connections"][0]
    oracle_conn = app.oracle_instance_connection(config_connection)

    assert oracle_conn.db_name == "test_db"
    assert oracle_conn.db_hostname == "localhost"
    assert oracle_conn.db_port == "1521"
    assert oracle_conn.db_user == "test_user"
    assert oracle_conn.db_passwd == "test_password"

def test_load_config_yaml(tmpdir):
    """Test the load_config_yaml function."""
    config_content = """
    global_config:
      log_level: INFO
      retry_conn_interval: 60
      default_time_interval: 15
      log_path: "logs/"
      port: 9844
    """
    config_file = tmpdir.join("config.yaml")
    config_file.write(config_content)

    config = app.load_config_yaml(str(config_file))
    assert config["global_config"]["log_level"] == "INFO"
    assert config["global_config"]["retry_conn_interval"] == 60

def test_get_labels_list(mock_config):
    """Test the get_labels_list function."""
    config_connections = mock_config["connections"]
    max_conn_labels = app.get_labels_list(config_connections)

    assert "dbhost" in max_conn_labels
    assert "dbport" in max_conn_labels
    assert "dbname" in max_conn_labels

def test_start_prometheus_exporter(mock_config, mock_prometheus_exporter):
    """Test the start_prometheus_exporter function."""
    config_queries = mock_config["queries"]
    max_conn_labels = app.get_labels_list(mock_config["connections"])
    port = 9877

    exporter = app.start_prometheus_exporter(config_queries, max_conn_labels, port)
    assert exporter is not None

@pytest.mark.asyncio
async def test_oracle_keep_connection(mock_oracle_connection):
    """Test the oracle_keep_connection function."""
    with patch("asyncio.sleep", return_value=None):
        await app.oracle_keep_connection(mock_oracle_connection, retry_conn_interval=1)
        mock_oracle_connection.connect.assert_called()

@pytest.mark.asyncio
async def test_query_set(mock_config, mock_oracle_connection, mock_prometheus_exporter):
    """Test the query_set function."""
    config_connection = mock_config["connections"][0]
    config_query = mock_config["queries"][0]
    default_time_interval = 15

    mock_oracle_connection.execute.return_value = [(1,)]

    await app.query_set(config_connection, mock_oracle_connection, config_query, mock_prometheus_exporter, default_time_interval)

    mock_oracle_connection.connect.assert_called()
    mock_oracle_connection.execute.assert_called_with(config_query["query"], config_query["name"])
    mock_prometheus_exporter.set_gauge.assert_called()

def test_main_success(mock_config, mock_oracle_connection, mock_prometheus_exporter, mock_event_loop):
    """Test the main function with a successful flow."""
    with patch("app.load_config_yaml", return_value=mock_config), \
         patch("app.setup_logging"), \
         patch("app.start_prometheus_exporter", return_value=mock_prometheus_exporter), \
         patch("app.oracle_instance_connection", return_value=mock_oracle_connection), \
         patch("app.asyncio.get_event_loop", return_value=mock_event_loop):

        # Run the main function
        with patch("sys.argv", ["app.py", "config.yaml"]):
            app.main()

        # Assertions
        app.load_config_yaml.assert_called_once_with("config.yaml")
        app.setup_logging.assert_called_once()
        app.start_prometheus_exporter.assert_called_once()
        app.oracle_instance_connection.assert_called_once()
        mock_event_loop.run_until_complete.assert_called_once()

def test_main_config_error(mock_config):
    """Test the main function with a configuration error."""
    with patch("app.load_config_yaml", side_effect=Exception("Config error")):
        with patch("sys.argv", ["app.py", "config.yaml"]):
            with pytest.raises(SystemExit):
                app.main()

def test_main_prometheus_error(mock_config, mock_prometheus_exporter):
    """Test the main function with a Prometheus exporter error."""
    with patch("app.load_config_yaml", return_value=mock_config), \
         patch("app.setup_logging"), \
         patch("app.start_prometheus_exporter", side_effect=Exception("Prometheus error")):

        with patch("sys.argv", ["app.py", "config.yaml"]):
            with pytest.raises(SystemExit):
                app.main()

def test_main_oracle_error(mock_config, mock_prometheus_exporter):
    """Test the main function with an Oracle connection error."""
    with patch("app.load_config_yaml", return_value=mock_config), \
         patch("app.setup_logging"), \
         patch("app.start_prometheus_exporter", return_value=mock_prometheus_exporter), \
         patch("app.oracle_instance_connection", side_effect=Exception("Oracle error")):

        with patch("sys.argv", ["app.py", "config.yaml"]):
            with pytest.raises(SystemExit):
                app.main()

def test_main_keyboard_interrupt(mock_config, mock_oracle_connection, mock_prometheus_exporter, mock_event_loop):
    """Test the main function handling a KeyboardInterrupt."""
    with patch("app.load_config_yaml", return_value=mock_config), \
         patch("app.setup_logging"), \
         patch("app.start_prometheus_exporter", return_value=mock_prometheus_exporter), \
         patch("app.oracle_instance_connection", return_value=mock_oracle_connection), \
         patch("app.asyncio.get_event_loop", return_value=mock_event_loop):

        # Simulate a KeyboardInterrupt during event loop execution
        mock_event_loop.run_until_complete.side_effect = KeyboardInterrupt

        with patch("sys.argv", ["app.py", "config.yaml"]):
            app.main()

        # Assertions
        app.load_config_yaml.assert_called_once_with("config.yaml")
        app.setup_logging.assert_called_once()
        app.start_prometheus_exporter.assert_called_once()
        app.oracle_instance_connection.assert_called_once()
        mock_event_loop.run_until_complete.assert_called_once()