import unittest
from unittest.mock import patch, MagicMock
from oraProm.ora import OracleConnection
import oracledb

class TestOracleConnection(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.db_name = "test_db"
        self.db_hostname = "localhost"
        self.db_port = "1521"
        self.db_user = "test_user"
        self.db_passwd = "test_password"
        self.conn = OracleConnection(self.db_name, self.db_hostname, self.db_port, self.db_user, self.db_passwd)

    @patch("oracledb.connect")
    def test_connect_success(self, mock_connect):
        """Test successful connection to Oracle database."""
        mock_connect.return_value = MagicMock()
        self.conn.connect()
        self.assertIsNotNone(self.conn.conn)
        mock_connect.assert_called_once()

    @patch("oracledb.connect")
    def test_connect_failure(self, mock_connect):
        """Test connection failure to Oracle database."""
        mock_connect.side_effect = Exception("Connection failed")
        self.conn.connect()
        self.assertIsNone(self.conn.conn)

    @patch("oracledb.Cursor")
    def test_execute_query_success(self, mock_cursor):
        """Test successful execution of a query."""
        mock_cursor_instance = MagicMock()
        mock_cursor_instance.fetchall.return_value = [(1,)]
        mock_cursor.return_value = mock_cursor_instance

        self.conn.conn = MagicMock()
        self.conn.conn.cursor.return_value = mock_cursor_instance

        result = self.conn.execute("SELECT 1 FROM dual", "test_query")
        self.assertEqual(result, [(1,)])

    def test_execute_query_no_connection(self):
        """Test query execution when no connection is established."""
        result = self.conn.execute("SELECT 1 FROM dual", "test_query")
        self.assertEqual(result, [])

    def test_execute_non_select_query(self):
        """Test execution of a non-SELECT query."""
        self.conn.conn = MagicMock()
        result = self.conn.execute("INSERT INTO test_table VALUES (1)", "test_query")
        self.assertEqual(result, [])

    @patch("oracledb.Connection.close")
    def test_close_connection_success(self, mock_close):
        """Test successful closing of the connection."""
        self.conn.conn = MagicMock()
        self.conn.close()
        mock_close.assert_called_once()

    def test_close_no_connection(self):
        """Test closing when no connection is established."""
        self.conn.close()  # Should not raise an error

if __name__ == "__main__":
    unittest.main()