import logging
import random
from common.timing_wrapper import TimingWrapper
from locust import User, task, between
from common.workload_config import workload_config

""" A simple workload that selects single rows
"""
class SelectWorkload(User):
    def __init__(self, environment):
        super().__init__(environment)
        self.conn = None

    # Wait time between each task
    wait_time = between(0.1, 0.5)

    @task(99)
    def read_row(self):
        # Reads from a random table
        table_name = workload_config.cluster_helper.get_random_table_name(workload_config.num_tables)
        try:
            with self.conn.cursor() as curs:
                ret, latency_ms = TimingWrapper(self.environment, curs, 'select').execute(f'SELECT count(*) FROM {table_name}')
                logging.info(f'Selected from table {table_name}. Latency: {int(latency_ms)} ms')
        except Exception as e:
            logging.error(f'Failed to select from row: {e}')

    @task(1)
    def change_app(self):
        # If only one database, this is a no-op
        if (self.conn is not None and workload_config.num_databases == 1):
            return

        # Change the tenant app
        if (self.conn is not None):
            self.conn.close()

        dbname = workload_config.cluster_helper.get_random_db_name(workload_config.num_databases)
        self.conn, latency_ms = TimingWrapper(self.environment, workload_config.cluster_helper.ybconnection, 'connect').connect_to_ysql(dbname)
        logging.info(f'Connected to database {dbname}. Latency: {int(latency_ms)}')

    def on_start(self):
        self.change_app()

    def on_stop(self):
        if (self.conn is not None):
            self.conn.close()
            self.conn = None