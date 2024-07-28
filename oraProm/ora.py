import oracledb
import logging

# Configure the logger
logger = logging.getLogger(__name__)

# Define the application name for logging purposes
APPLICATION_NAME = "DB2PROM"

class OracleConnection:
    def connect(self):
        """
        Establish a connection to the Oracle database using the provided connection details.
        """
        try:
            if not self.conn:
                # Create a DSN (Data Source Name) for the Oracle connection
                dsn = oracledb.makedsn(self.db_hostname, self.db_port, service_name=self.db_name)
                # Establish the connection using the DSN and user credentials
                self.conn = oracledb.connect(user=self.db_user, password=self.db_passwd, dsn=dsn)
                # Set autocommit to true for the connection
                self.conn.autocommit = True
                logger.info("[{}] connected".format(self.connection_string_print))
        except KeyboardInterrupt as e:
            # Handle keyboard interrupt to allow graceful termination
            raise e
        except oracledb.DatabaseError as e:
            # Log specific Oracle database errors
            error, = e.args
            logger.error("[{}] Oracle Database error: {}".format(self.connection_string_print, error.message))
            self.conn = None
        except Exception as e:
            # Log general exceptions
            logger.error("[{}] Connection error: {}".format(self.connection_string_print, e))
            self.conn = None

    def __init__(self, db_name: str, db_hostname: str, db_port: str, db_user: str, db_passwd: str):
        """
        Initialize the OracleConnection instance with the necessary database connection details.
        """
        self.db_name = db_name
        self.db_hostname = db_hostname
        self.db_port = db_port
        self.db_user = db_user
        self.db_passwd = db_passwd
        self.connection_string_print = "{}:{}/{}".format(db_hostname, db_port, db_name)
        self.conn = None

    def execute(self, query: str, name: str):
        """
        Execute a SQL SELECT query on the Oracle database and return the results.
        Ensure only SELECT queries are executed.
        """
        try:
            if not self.conn:
                logger.warning("[{}] No active connection to execute query: [{}]".format(self.connection_string_print, name))
                return []
            
            # Ensure the query is a SELECT statement
            if not query.strip().lower().startswith("select"):
                logger.error("[{}] [{}] Only SELECT queries are allowed. Attempted query: {}".format(self.connection_string_print, name, query))
                return []
            
            # Create a cursor to execute the SQL query
            cursor = self.conn.cursor()
            cursor.execute(query)
            logger.debug("[{}] [{}] executed".format(self.connection_string_print, name))
            
            # Fetch all rows from the executed query
            rows = cursor.fetchall()
            cursor.close()
            return rows
        except KeyboardInterrupt as e:
            # Handle keyboard interrupt to allow graceful termination
            raise e
        except oracledb.DatabaseError as e:
            # Log specific Oracle database errors during query execution
            error, = e.args
            logger.warning("[{}] [{}] Oracle Database error: {}".format(self.connection_string_print, name, error.message))
            return [[]]
        except Exception as e:
            # Log general exceptions during query execution
            logger.warning("[{}] [{}] Failed to execute: {}".format(self.connection_string_print, name, e))
            return [[]]

    def close(self):
        """
        Close the connection to the Oracle database.
        """
        try:
            if self.conn:
                # Close the connection
                self.conn.close()
                self.conn = None
                logger.info("[{}] closed".format(self.connection_string_print))
        except oracledb.DatabaseError as e:
            # Log specific Oracle database errors during connection close
            error, = e.args
            logger.error("[{}] Failed to close connection: Oracle Database error: {}".format(self.connection_string_print, error.message))
        except Exception as e:
            # Log general exceptions during connection close
            logger.error("[{}] Failed to close connection: {}".format(self.connection_string_print, e))

# # Example usage
# if __name__ == "__main__":
#     logging.basicConfig(level=logging.DEBUG)
#     conn = OracleConnection(db_name="orcl", db_hostname="localhost", db_port="1521", db_user="user", db_passwd="passwd")
#     conn.connect()
#     results = conn.execute("SELECT * FROM some_table", "TestQuery")
#     for row in results:
#         print(row)
#     conn.close()
