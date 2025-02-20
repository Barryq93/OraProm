import oracledb
import logging

# Configure the logger
logger = logging.getLogger(__name__)

class OracleConnection:
    def __init__(self, db_name: str, db_hostname: str, db_port: str, db_user: str, db_passwd: str):
        """Initialize the Oracle connection."""
        self.db_name = db_name
        self.db_hostname = db_hostname
        self.db_port = db_port
        self.db_user = db_user
        self.db_passwd = db_passwd
        self.connection_string_print = f"{db_hostname}:{db_port}/{db_name}"
        self.conn = None

    def connect(self):
        """Establish a connection to the Oracle database."""
        try:
            if not self.conn:
                dsn = oracledb.makedsn(self.db_hostname, self.db_port, service_name=self.db_name)
                self.conn = oracledb.connect(user=self.db_user, password=self.db_passwd, dsn=dsn)
                self.conn.autocommit = True
                logger.info(f"[{self.connection_string_print}] connected")
        except oracledb.DatabaseError as e:
            error, = e.args
            logger.error(f"[{self.connection_string_print}] Oracle Database error: {error.message}")
            self.conn = None
        except Exception as e:
            logger.error(f"[{self.connection_string_print}] Connection error: {e}")
            self.conn = None

    def execute(self, query: str, name: str):
        """Execute a SQL query."""
        try:
            if not self.conn:
                logger.warning(f"[{self.connection_string_print}] No active connection to execute query: [{name}]")
                return []

            if not query.strip().lower().startswith("select"):
                logger.error(f"[{self.connection_string_print}] [{name}] Only SELECT queries are allowed. Attempted query: {query}")
                return []

            cursor = self.conn.cursor()
            cursor.execute(query)
            logger.debug(f"[{self.connection_string_print}] [{name}] executed")

            rows = cursor.fetchall()
            cursor.close()
            return rows
        except oracledb.DatabaseError as e:
            error, = e.args
            logger.warning(f"[{self.connection_string_print}] [{name}] Oracle Database error: {error.message}")
            return [[]]
        except Exception as e:
            logger.warning(f"[{self.connection_string_print}] [{name}] Failed to execute: {e}")
            return [[]]

    def close(self):
        """Close the Oracle connection."""
        try:
            if self.conn:
                self.conn.close()
                self.conn = None
                logger.info(f"[{self.connection_string_print}] closed")
        except oracledb.DatabaseError as e:
            error, = e.args
            logger.error(f"[{self.connection_string_print}] Failed to close connection: Oracle Database error: {error.message}")
        except Exception as e:
            logger.error(f"[{self.connection_string_print}] Failed to close connection: {e}")