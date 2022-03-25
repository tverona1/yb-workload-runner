import gevent
import logging
from locust.env import Environment
from locust.stats import stats_printer, stats_history
from .workload_config import workload_config

class Executor:
    def __init__(self, cluster_helper, workloads):

        # Setup Enviroment and Runner
        self.env = Environment(user_classes=workloads)
        self.env.create_local_runner()

        # Setup workload config
        workload_config.cluster_helper = cluster_helper
        workload_config.num_databases = cluster_helper.get_num_databases()
        workload_config.num_tables = cluster_helper.get_num_tables(cluster_helper.get_db_name(1))

        logging.info(f'Number of database: {workload_config.num_databases}, number of tables: {workload_config.num_tables}')

        # Start web UI instance
        # self.env.create_web_ui("127.0.0.1", 8089)

        # Start a greenlet that periodically outputs the current stats
        #gevent.spawn(stats_printer(self.env.stats))

        # Start a greenlet that save current stats to history
        gevent.spawn(stats_history, self.env.runner)

    def execute(self, num_users, spawn_rate, execution_time):
        # Start the test
        self.env.runner.start(num_users, spawn_rate=spawn_rate)

        # Stop the runner after timeout
        gevent.spawn_later(execution_time, lambda: self.env.runner.quit())

        # Wait for the greenlets
        self.env.runner.greenlet.join()

        # Stop the web server
        #self.env.web_ui.stop()
