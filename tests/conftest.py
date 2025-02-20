import pytest
import yaml
from unittest.mock import MagicMock
from oraProm.ora import OracleConnection
from oraProm.prometheus import CustomExporter

@pytest.fixture
def mock_config():
    """Fixture to provide a mock configuration."""
    return {
        "global_config": {
            "log_level": "INFO",
            "retry_conn_interval": 60,
            "default_time_interval": 15,
            "log_path": "logs/",
            "port": 9844,
        },
        "connections": [
            {
                "db_host": "localhost",
                "db_name": "test_db",
                "db_port": "1521",
                "db_user": "test_user",
                "db_passwd": "test_password",
            }
        ],
        "queries": [
            {
                "name": "test_query",
                "query": "SELECT 1 FROM dual",
                "gauges": [
                    {
                        "name": "test_gauge",
                        "desc": "Test gauge",
                        "col": 1,
                    }
                ],
            }
        ],
    }

@pytest.fixture
def mock_oracle_connection():
    """Fixture to provide a mock OracleConnection instance."""
    oracle_conn = OracleConnection(
        db_name="test_db",
        db_hostname="localhost",
        db_port="1521",
        db_user="test_user",
        db_passwd="test_password",
    )
    oracle_conn.conn = MagicMock()  # Mock the actual connection
    return oracle_conn

@pytest.fixture
def mock_prometheus_exporter():
    """Fixture to provide a mock CustomExporter instance."""
    exporter = CustomExporter(port=9877)
    exporter.metric_dict = MagicMock()  # Mock the metric dictionary
    return exporter

@pytest.fixture
def mock_event_loop():
    """Fixture to provide a mock asyncio event loop."""
    loop = MagicMock()
    return loop