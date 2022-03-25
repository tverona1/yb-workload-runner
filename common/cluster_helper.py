import logging
import random

class ClusterHelper:
    DB_NAME_PREFIX = 'md_scalability_db_'
    TABLE_NAME_PREFIX = 'md_scalability_table_'

    def __init__(self, ybconnection, init_dbname):
        self.ybconnection = ybconnection
        self.init_dbname = init_dbname

    # Constructs a db name given an id
    def get_db_name(self, id):
        return self.DB_NAME_PREFIX + str(id)

    # Constructs a table name given an id
    def get_table_name(self, id):
        return self.TABLE_NAME_PREFIX + str(id)

    # Constructs a random table name given table count
    def get_random_table_name(self, num_tables):
        return self.get_table_name(random.randrange(1, num_tables + 1))

    # Constructs a random db name given table count
    def get_random_db_name(self, num_dbs):
        return self.get_db_name(random.randrange(1, num_dbs + 1))

    def _create_databases(self, ysql_session, num_databases):
        for x in range(1, num_databases + 1):
            name = self.get_db_name(x)
            try:
                with ysql_session.cursor() as curs:
                    curs.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{name}'")
                    exists = curs.fetchone()
                    if not exists:
                        curs.execute(f'CREATE DATABASE {name} WITH colocated = true')
                        logging.info(f'Created database {name}')
                    else:
                        logging.info(f'Skipping creation of database {name} because it already exists')
            except Exception as e:
                raise Exception(f'Error creating database: {e}') from e

    def _create_database_objects(self, ysql_session, dbname, num_tables):
        for x in range(1, num_tables + 1):
            table_name = self.get_table_name(x)
            try:
                with ysql_session.cursor() as curs:
                    curs.execute(f'CREATE TABLE IF NOT EXISTS {table_name} (k SERIAL PRIMARY KEY, v1 VARCHAR, v2 INT, v3 TEXT)')
                    logging.info(f'Created table {table_name} on database {dbname}')
            except Exception as e:
                raise Exception(f'Error creating table: {e}') from e

            try:
                with ysql_session.cursor() as curs:
                    curs.execute(f'CREATE INDEX ON {table_name} (v1)')
                    logging.info(f'Created index on table {table_name}, database {dbname}')
            except Exception as e:
                raise Exception(f'Error creating table {table_name} on databse {dbname}: {e}') from e

    def setup_cluster(self, num_databases, num_tables):
        # Create databases
        default_conn = self.ybconnection.connect_to_ysql(self.init_dbname)
        try:
            self._create_databases(default_conn, num_databases)
        finally:
            default_conn.close()
            
        # Create database objects
        for x in range(1, num_databases + 1):
            dbname = self.get_db_name(x)
            conn = self.ybconnection.connect_to_ysql(dbname)
            try:
                self._create_database_objects(conn, dbname, num_tables)
            finally:
                conn.close()

    def clean_cluster(self):
        # Drop until we exhaust the number of databases
        conn = self.ybconnection.connect_to_ysql(self.init_dbname)
        db_num = 1
        try:
            while(True):
                dbname = self.get_db_name(db_num)
                try:
                    with conn.cursor() as curs:
                        curs.execute(f'DROP DATABASE {dbname}')
                        logging.info(f'Dropped database {dbname}')
                except Exception as e:
                    break
                db_num = db_num + 1
        finally:
            conn.close()

    def get_num_databases(self):
        conn = self.ybconnection.connect_to_ysql(self.init_dbname)
        try:
            with conn.cursor() as curs:
                curs.execute(f"SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname LIKE '{self.DB_NAME_PREFIX}%'")
                result = curs.fetchone()
                if (result):
                    return result[0]
                else:
                    return 0
        finally:
            conn.close()

    def get_num_tables(self, dbname):
        conn = self.ybconnection.connect_to_ysql(dbname)
        try:
            with conn.cursor() as curs:
                curs.execute(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE table_name LIKE '{self.TABLE_NAME_PREFIX}%'")
                result = curs.fetchone()
                if (result):
                    return result[0]
                else:
                    return 0
        finally:
            conn.close()
