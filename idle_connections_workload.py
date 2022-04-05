import logging
from common.timing_wrapper import TimingWrapper
from locust import User, task
from common.workload_config import workload_config

# Track number of connections for logging purposes
num_connections = 0

""" Spawn idle connections
"""
class IdleConnectionsWorkload(User):
    def __init__(self, environment):
        super().__init__(environment)
        self.conn = None
        self.ybconnection = TimingWrapper(environment, workload_config.cluster_helper.ybconnection, "idle_connection")

    @task
    def dummy_task(self):
        """ Dummy task """

    def connect(self):
        global num_connections

        # Connect to database
        dbname = workload_config.cluster_helper.get_db_name(1)
        self.conn, latency_ms = self.ybconnection.connect_to_ysql(dbname)

        logging.info(f'Spawned connection #{num_connections}. Latency: {int(latency_ms)} ms')
        num_connections = num_connections + 1

    def on_start(self):
        self.connect()

    def on_stop(self):
        global num_connections
        num_connections = num_connections - 1
        logging.info(f'Closed connection #{num_connections}')
        if (self.conn is not None):
            self.conn.close()
            self.conn = None