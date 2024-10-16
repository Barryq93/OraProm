import os
import sys
import signal
import argparse
import yaml
import logging
import asyncio
import re
from logging.handlers import RotatingFileHandler
from oraProm.ora import OracleConnection  # Import OracleConnection class from ora.py
from oraProm.prometheus import CustomExporter, INVALID_LABEL_STR  # Assuming these are defined in prometheus.py

def setup_logging(log_path, log_level):
    os.makedirs(log_path, exist_ok=True)
    logger = logging.getLogger()
    logger.setLevel(log_level)

    log_file = os.path.join(log_path, "oraProm.log")
    handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    error_log_file = os.path.join(log_path, "db2prom.err")
    error_handler = RotatingFileHandler(error_log_file, maxBytes=10*1024*1024, backupCount=5)
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)

def oracle_instance_connection(config_connection):
    logging.info("Setting up Oracle connection with provided configuration.")
    conn = {
        "db_name": config_connection["db_name"],
        "db_hostname": config_connection["db_host"],
        "db_port": config_connection["db_port"],
        "db_user": config_connection["db_user"],
        "db_passwd": config_connection["db_passwd"]
    }

    for key, value in conn.items():
        if not value:
            logging.fatal(f"Missing {key} field for connection.")
            sys.exit(1)

    return OracleConnection(**conn)

async def oracle_keep_connection(oracle_conn, retry_conn_interval=60):
    logging.info(f"Starting Oracle connection keeper with retry interval {retry_conn_interval} seconds.")
    while True:
        try:
            oracle_conn.connect()
        except Exception as e:
            logging.error(f"Error keeping Oracle connection: {e}")
        await asyncio.sleep(retry_conn_interval)

async def query_set(config_connection, oracle_conn, config_query, exporter, default_time_interval):
    logging.info(f"Starting query set for: {config_query['name']} with interval {config_query.get('time_interval', default_time_interval)} seconds.")
    time_interval = config_query.get("time_interval", default_time_interval)

    while True:
        try:
            oracle_conn.close()
            oracle_conn.connect()

            c_labels = {
                "dbhost": config_connection["db_host"],
                "dbport": config_connection["db_port"],
                "dbname": config_connection["db_name"],
            }
            if "extra_labels" in config_connection:
                c_labels.update(config_connection["extra_labels"])

            max_conn_labels = {"dbhost", "dbenv", "dbname", "dbinstance", "dbport"}
            c_labels = {i: INVALID_LABEL_STR for i in max_conn_labels} | c_labels

            res = oracle_conn.execute(config_query["query"], config_query["name"])
            g_counter = 0
            for g in config_query["gauges"]:
                if "extra_labels" in g:
                    g_labels = g["extra_labels"]
                else:
                    g_labels = {}

                if "col" in g:
                    col = int(g["col"]) - 1
                else:
                    col = g_counter

                has_special_labels = any(re.match(r'^\$\d+$', v) for v in g_labels.values())

                if not has_special_labels:
                    if res:
                        row = res[0]
                        labels = g_labels | c_labels
                        if row and len(row) >= col:
                            exporter.set_gauge(g["name"], row[col], labels)
                else:
                    for row in res:
                        g_labels_aux = g_labels.copy()
                        for k, v in g_labels_aux.items():
                            g_label_index = int(re.match('^\$(\d+)$', v).group(1)) - 1 if re.match('^\$(\d+)$', v) else 0
                            g_labels_aux[k] = row[g_label_index] if row and len(row) >= g_label_index else INVALID_LABEL_STR
                        labels = g_labels_aux | c_labels
                        if row and len(row) >= col:
                            exporter.set_gauge(g["name"], row[col], labels)
                g_counter += 1
        except Exception as e:
            logging.error(f"Error executing query {config_query['name']}: {e}")
        finally:
            await asyncio.sleep(time_interval)

def load_config_yaml(file_str):
    logging.info(f"Loading configuration file: {file_str}")
    try:
        with open(file_str, "r") as f:
            file_dict = yaml.safe_load(f)
            if not isinstance(file_dict, dict):
                logging.fatal(f"Could not parse '{file_str}' as dict")
                sys.exit(1)
            return file_dict
    except yaml.YAMLError as e:
        logging.fatal(f"File {file_str} is not a valid YAML: {e}")
        sys.exit(1)
    except FileNotFoundError:
        logging.fatal(f"File {file_str} not found")
        sys.exit(1)
    except Exception as e:
        logging.fatal(f"Could not open file {file_str}: {e}")
        sys.exit(1)

def get_labels_list(config_connections):
    max_conn_labels = set()
    for c in config_connections:
        if "extra_labels" in c:
            c_labels = c["extra_labels"]
        else:
            c_labels = {}
        max_conn_labels |= set(c_labels)
    max_conn_labels.add("dbhost")
    max_conn_labels.add("dbport")
    max_conn_labels.add("dbname")
    return max_conn_labels

def start_prometheus_exporter(config_queries, max_conn_labels, port):
    logging.info(f"Starting Prometheus exporter on port {port} and initializing metrics.")
    try:
        custom_exporter = CustomExporter(port=port)
        for q in config_queries:
            if "gauges" not in q:
                raise Exception(f"{q} is missing 'gauges' key")
            for g in q["gauges"]:
                labels = g.get("extra_labels", {}).keys()
                labels = list(max_conn_labels | set(labels))
                name = g.get("name")
                if not name:
                    raise Exception("Some gauge metrics are missing name")
                desc = g.get("desc", "")
                custom_exporter.create_gauge(name, desc, labels)
        custom_exporter.start()
        return custom_exporter
    except Exception as e:
        logging.fatal(f"Could not start/init Prometheus Exporter server: {e}")
        raise e

async def main(config_connection, config_queries, exporter, default_time_interval, port):
    executions = []
    try:
        oracle_conn = oracle_instance_connection(config_connection)
        retry_connect_interval = config_connection.get("retry_conn_interval", 60)
        executions.append(oracle_keep_connection(oracle_conn, retry_connect_interval))

        for q in config_queries:
            if "query" not in q:
                raise Exception(f"{q} is missing 'query' key")
            executions.append(query_set(config_connection, oracle_conn, q, exporter, default_time_interval))

        await asyncio.gather(*executions)
    except KeyboardInterrupt:
        logging.info("Received KeyboardInterrupt, shutting down.")
        return None

def signal_handler(sig, frame):
    logging.info("Received termination signal, shutting down gracefully.")
    loop.stop()
    sys.exit(0)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Oracle Prometheus Exporter')
    parser.add_argument('config_file', type=str, help='Path to the config YAML file')
    args = parser.parse_args()

    if not args.config_file:
        logging.error("Error: Configuration file argument is missing.")
        sys.exit(1)

    try:
        config = load_config_yaml(args.config_file)
        logging.info(f"Loaded config: {config}")

        global_config = config["global_config"]
        log_level = logging.getLevelName(global_config.get("log_level", "INFO"))
        log_path = global_config.get("log_path", "/path/to/logs/")
        port = global_config.get("port", 9844)
        retry_conn_interval = global_config.get("retry_conn_interval", 60)
        logging.info(f"Retry connection interval: {retry_conn_interval}")

        setup_logging(log_path, log_level)
        logging.info("Configuration file loaded successfully.")

        for current_variable in ["retry_conn_interval", "default_time_interval"]:
            if int(global_config.get(current_variable, 15)) < 1:
                logging.fatal(f"Invalid value for {current_variable}")
                sys.exit(2)

        config_connections = config["connections"]
        config_queries = config["queries"]

        max_conn_labels = get_labels_list(config_connections)
        logging.info(f"Max connection labels: {max_conn_labels}")

        exporter = start_prometheus_exporter(config_queries, max_conn_labels, port)

        signal.signal(signal.SIGINT, signal_handler)

        try:
            loop = asyncio.get_event_loop()
            tasks = []
            for config_connection in config_connections:
                tasks.append(main(config_connection, config_queries, exporter, int(global_config["default_time_interval"]), port))
            loop.run_until_complete(asyncio.gather(*tasks))
        except KeyboardInterrupt:
            logging.info("Received KeyboardInterrupt, shutting down.")
    except KeyError as ke:
        logging.critical(f"{ke.args[0]} not found in global_config. Check configuration.")
        sys.exit(1)
    except Exception as e:
        logging.critical(f"Error loading configuration: {e}. Check configuration.")
        sys.exit(1)
