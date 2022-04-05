import socket
import psycopg2
import psycopg2.extras

class YBConnection:
    def __init__(self, host, port, dbuser, dbpassword, useipv6 = False):
        """Initialize connection object

        Args:
            host (str): Host (if None, default to localhost)
            port (str): Port (if None, default to 5433)
            dbuser (str): User name
            dbpassword (str): Password
            useipv6 (bool, optional): Whether to use ipv6. Defaults to False.
        """
        self.dbuser = dbuser
        self.dbpassword = dbpassword
        self.useipv6 = useipv6
        self.ysql_session = None
        self.host = host if host is not None else self._get_local_ipaddr()
        self.port = port if port is not None else 5433

    def _get_local_ipaddr(self):
        # Get the local ipv4 or ipv6 address of YW API
        addr_family = socket.AF_INET
        sock = socket.socket(addr_family, socket.SOCK_DGRAM)
        try:
            sock.connect((socket.gethostname(), 1))
            api_address = sock.getsockname()[0]
        except socket.error:
            api_address = '::1' if self.useipv6 else '127.0.0.1'
        finally:
            sock.close()
        return api_address

    def connect_to_ysql(self, dbname):
        # Connect to ysql
        sslmode = None
        sslrootcert = None
        sslcert = None
        sslkey = None
        conn = None
        try:
            conn = psycopg2.connect(database=dbname,
                                    user=self.dbuser,
                                    password=self.dbpassword,
                                    host=self.host,
                                    port=self.port,
                                    sslmode=sslmode,
                                    sslrootcert=sslrootcert,
                                    sslcert=sslcert,
                                    sslkey=sslkey)

            # Default to auto-commit
            conn.set_session(autocommit=True)
        except psycopg2.OperationalError as pg_ex:
            raise Exception(f'Failed to connect to YSQL: {pg_ex}') from pg_ex
        return conn