import unittest
from unittest.mock import patch, MagicMock
from oraProm.prometheus import CustomExporter
from prometheus_client import Gauge

class TestCustomExporter(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.exporter = CustomExporter(port=9877)

    @patch("prometheus_client.Gauge")
    def test_create_gauge_success(self, mock_gauge):
        """Test successful creation of a gauge."""
        mock_gauge_instance = MagicMock()
        mock_gauge.return_value = mock_gauge_instance

        self.exporter.create_gauge("test_metric", "Test metric description", ["label1", "label2"])
        self.assertIn("test_metric", self.exporter.metric_dict)
        mock_gauge.assert_called_once_with("test_metric", "Test metric description", ["label1", "label2"])

    def test_set_gauge_success(self):
        """Test successful setting of a gauge value."""
        mock_gauge = MagicMock()
        self.exporter.metric_dict["test_metric"] = mock_gauge

        self.exporter.set_gauge("test_metric", 123.45, {"label1": "value1"})
        mock_gauge.labels.assert_called_once_with(label1="value1")
        mock_gauge.labels.return_value.set.assert_called_once_with(123.45)

    def test_set_gauge_no_labels(self):
        """Test setting a gauge value without labels."""
        mock_gauge = MagicMock()
        self.exporter.metric_dict["test_metric"] = mock_gauge

        self.exporter.set_gauge("test_metric", 123.45)
        mock_gauge.set.assert_called_once_with(123.45)

    def test_set_gauge_metric_not_found(self):
        """Test setting a gauge value for a non-existent metric."""
        with self.assertLogs(level="ERROR"):
            self.exporter.set_gauge("non_existent_metric", 123.45)

    @patch("prometheus_client.start_http_server")
    def test_start_exporter_success(self, mock_start_http_server):
        """Test successful start of the Prometheus exporter."""
        self.exporter.start()
        mock_start_http_server.assert_called_once_with(9877)

    @patch("prometheus_client.start_http_server")
    def test_start_exporter_failure(self, mock_start_http_server):
        """Test failure to start the Prometheus exporter."""
        mock_start_http_server.side_effect = Exception("Failed to start server")
        with self.assertRaises(Exception):
            self.exporter.start()

if __name__ == "__main__":
    unittest.main()