import logging
import random
from locust import User, task, between
from common.workload_config import workload_config

""" A simple workload that inserts values and reads single rows
"""
class SimpleTenantApp(User):
    def __init__(self, environment):
        super().__init__(environment)
        self.conn = None

    # Wait between 0.5 to 2 secs between each task
    wait_time = between(0.5, 2)

    def _random_string(self,length):
       return ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for i in range(length))

    @task(40)
    def insert_row(self):
        # Inserts a row into a random table
        table_name = workload_config.cluster_helper.get_random_table_name(workload_config.num_tables)
        try:
            with self.conn.cursor() as curs:
                curs.execute(f"INSERT INTO {table_name} (v1, v2, v3) VALUES ('{self._random_string(40)}', {random.randrange(1000)}, '{self._random_string(100)}')")
                logging.info(f'Inserted row into table {table_name}')
        except Exception as e:
            logging.error(f'Failed to insert row: {e}')

    @task(60)
    def read_row(self):
        # Reads from a random table
        table_name = workload_config.cluster_helper.get_random_table_name(workload_config.num_tables)
        try:
            with self.conn.cursor() as curs:
                curs.execute(f'SELECT count(*) FROM {table_name}')
                logging.info(f'Selected from  table {table_name}')
        except Exception as e:
            logging.error(f'Failed to select from row: {e}')

    @task(1)
    def change_app(self):
        # Change the tenant app
        if (self.conn is not None):
            self.conn.close()

        dbname = workload_config.cluster_helper.get_random_db_name(workload_config.num_databases)
        self.conn = workload_config.cluster_helper.ybconnection.connect_to_ysql(dbname)
        logging.info(f'Connected to database {dbname}')

    def on_start(self):
        self.change_app()

    def on_stop(self):
        if (self.conn is not None):
            self.conn.close()