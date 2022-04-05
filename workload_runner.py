import argparse
import logging
from common.ybconnection import YBConnection
from common.cluster_helper import ClusterHelper
from common.executor import Executor
from simple_workload import SimpleWorkload
from idle_connections_workload import IdleConnectionsWorkload

class Main:
    # Example usage (see below for overrides):
    # Setup: python3 workload_runner.py setup --num_databases 1 --num_tables 500
    # Execute: python3 workload_runner.py execute --num_users 1000 --spawn_rate 20 --execution_time 600
    # Clean up: python3 workload_runner.py cleanup

    def __init__(self):
        # List of available workloads
        self.workloads = {'idle_connections': IdleConnectionsWorkload, 'simple' : SimpleWorkload}

    def parse_arguments(self):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument('--host', default=None,
                            help="Host (default to localhost if not specified)")
        parser.add_argument('--port', default=None,
                            help="Port (default to 5433 if not specified)")
        parser.add_argument('--dbuser', default="yugabyte",
                            help="Database user to connect as")
        parser.add_argument('--dbpass', default="yugabyte",
                            help="Password for dbuser")
        parser.add_argument('--ipv6', action='store_true',
                            help="Use ipv6 (default is false)")
        parser.add_argument('--initialdb', default='yugabyte',
                            type=str.lower,
                            help="Initial database to connect to")

        subparsers = parser.add_subparsers(dest='command')

        parser_setup_cluster = subparsers.add_parser('setup',
                            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                            help="Setup cluster by creating databases and tables")
        parser_setup_cluster.add_argument('--num_databases', required=True,
                            type=int,
                            help="Number of databases to create")
        parser_setup_cluster.add_argument('--num_tables', required=True,
                            type=int,
                            help="Number of tables to create per database")

        subparsers.add_parser('cleanup',
                            help="Cleanup cluster")

        parser_execute_workload = subparsers.add_parser('execute',
                            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                            help="Execute the workload")
        parser_execute_workload.add_argument('--workload',
                            choices=self.workloads.keys(), required=True,
                            help="Workload to run")
        parser_execute_workload.add_argument('--num_users', default=100,
                            type=int,
                            help="Number of users")
        parser_execute_workload.add_argument('--spawn_rate', default=10,
                            type=int,
                            help="Spawn rate (users / sec)")
        parser_execute_workload.add_argument('--execution_time', default=600,
                            type=int,
                            help="Execution time in secs")
        parser_execute_workload.add_argument('--csv', default=None,
                            help="CSV file base name")

        args = parser.parse_args()

        if (args.command is None):
            parser.print_usage()
            print("error: the following arguments are required: command")
            return None

        return args

    def main(self):
        # Parse arguments
        args = self.parse_arguments()
        if (args is None):
            return

        # Create yb connection object
        ybconnection = YBConnection(args.host, args.port, args.dbuser, args.dbpass, args.ipv6)

        cluster_helper = ClusterHelper(ybconnection, args.initialdb)

        if (args.command == 'setup'):
            cluster_helper.setup_cluster(args.num_databases, args.num_tables)
        elif (args.command == 'cleanup'):
            cluster_helper.clean_cluster()
        elif (args.command == 'execute'):
            workload_runner = Executor(cluster_helper, [self.workloads[args.workload]], args.csv)
            workload_runner.execute(args.num_users, args.spawn_rate, args.execution_time)
        else:
            raise Exception(f'Unknown command {args.command}')

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    Main().main()
