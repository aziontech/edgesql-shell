import os
import pandas as pd
import mysql.connector
import psycopg2
from psycopg2 import sql, OperationalError
from halo import Halo
import utils

def is_remote(host):
    """
    Check if the host is remote.

    Args:
        host (str): The hostname or IP address.

    Returns:
        bool: True if the host is remote, False if it's localhost.
    """
    return host not in ['localhost', '127.0.0.1']

def connect_database(db_type, use_tls, connection_args):
    """
    Connect to a database based on type and connection arguments.

    Args:
        db_type (str): The type of the database ('mysql' or 'postgres').
        use_tls (bool): Whether to use TLS for the connection.
        connection_args (dict): Connection arguments for the database.

    Returns:
        Connection: A connection object to the database.

    Raises:
        ValueError: If the database type is invalid.
    """
    if db_type == 'mysql':
        return connect_mysql(use_tls, connection_args)
    elif db_type == 'postgres':
        return connect_postgres(use_tls, connection_args)
    else:
        raise ValueError("Invalid database type. Use 'mysql' or 'postgres'.")

def connect_mysql(use_tls, connection_args):
    """
    Connect to a MySQL database.

    Args:
        use_tls (bool): Whether to use TLS for the connection.
        connection_args (dict): Connection arguments for the MySQL database.

    Returns:
        Connection: A connection object to the MySQL database.

    Raises:
        OperationalError: If there is an error connecting to the database.
    """
    try:
        if use_tls:
            return mysql.connector.connect(**connection_args)
        else:
            return mysql.connector.connect(
                user=connection_args['user'],
                password=connection_args['password'],
                host=connection_args['host'],
                port=connection_args['port'],
                database=connection_args['database']
            )
    except mysql.connector.Error as e:
        raise OperationalError(f"Error connecting to MySQL: {e}") from e

def connect_postgres(use_tls, connection_args, timeout=60000):
    """
    Connect to a PostgreSQL database and set the statement timeout.

    Args:
        use_tls (bool): Whether to use TLS for the connection.
        connection_args (dict): Connection arguments for the PostgreSQL database.
        timeout (int): Statement timeout in milliseconds. Default is 60000 ms (60 seconds).

    Returns:
        Connection: A connection object to the PostgreSQL database.

    Raises:
        OperationalError: If there is an error connecting to the database.
    """
    try:
        conn = None
        if use_tls:
            conn = psycopg2.connect(**connection_args)
        else:
            conn = psycopg2.connect(
                user=connection_args['user'],
                password=connection_args['password'],
                host=connection_args['host'],
                port=connection_args['port'],
                database=connection_args['database']
            )

        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("SET statement_timeout = %s"), [timeout])
        
        return conn
    except psycopg2.Error as e:
        raise OperationalError(f"Error connecting to PostgreSQL: {e}") from e

def fetch_data_from_table_mysql(cursor, source_table, limit, offset):
    """
    Fetch data from a MySQL table.

    Args:
        cursor (Cursor): The MySQL cursor object.
        source_table (str): The name of the source table.
        limit (int): The maximum number of rows to fetch.
        offset (int): The offset for fetching rows.

    Returns:
        list: Fetched rows from the table.
        list: Description of the columns.
    """
    query = f"SELECT * FROM `{source_table}` LIMIT %s OFFSET %s"
    cursor.execute(query, (limit, offset))
    return cursor.fetchall(), cursor.description

def fetch_data_from_table_postgres(cursor, source_table, limit, offset):
    """
    Fetch data from a PostgreSQL table.

    Args:
        cursor (Cursor): The PostgreSQL cursor object.
        source_table (str): The name of the source table.
        limit (int): The maximum number of rows to fetch.
        offset (int): The offset for fetching rows.

    Returns:
        list: Fetched rows from the table.
        list: Description of the columns.
    """
    query = sql.SQL("SELECT * FROM {} LIMIT %s OFFSET %s").format(sql.Identifier(source_table))
    cursor.execute(query, (limit, offset))
    return cursor.fetchall(), cursor.description

def fetch_data_from_table(db_type, cursor, source_table, limit, offset):
    """
    Fetch data from a table in a database.

    Args:
        db_type (str): The type of the database ('mysql' or 'postgres').
        cursor (Cursor): The cursor object for the database.
        source_table (str): The name of the source table.
        limit (int): The maximum number of rows to fetch.
        offset (int): The offset for fetching rows.

    Returns:
        list: Fetched rows from the table.
        list: Description of the columns.

    Raises:
        ValueError: If the database type is unsupported.
    """
    if db_type == 'mysql':
        return fetch_data_from_table_mysql(cursor, source_table, limit, offset)
    elif db_type == 'postgres':
        return fetch_data_from_table_postgres(cursor, source_table, limit, offset)
    else:
        raise ValueError("Unsupported database type.")


def get_size_of_row(row, columns):
    """
    Calculate the size of a single row in bytes.

    Args:
        row (list): The row of data.
        columns (list): The column names.

    Returns:
        int: The size of the row in bytes.
    """
    df = pd.DataFrame([row], columns=columns)
    return df.memory_usage(index=True, deep=True).sum()

def importer(db_type, db_database, source_table, max_chunk_rows=512, max_chunk_size_mb=0.8):
    """
    Import data from a database in chunks.

    Args:
        db_type (str): The type of the database ('mysql' or 'postgres').
        db_database (str): The name of the database.
        source_table (str): The name of the source table.
        max_chunk_rows (int, optional): Maximum number of rows per chunk. Default is 512.
        max_chunk_size_mb (float, optional): Maximum size of each chunk in megabytes. Default is 0.8 MB.

    Returns:
        generator: A generator yielding DataFrame chunks of data.
    """
    db_user = os.environ.get(f'{db_type.upper()}_USERNAME')
    db_password = os.environ.get(f'{db_type.upper()}_PASSWORD')
    db_host = os.environ.get(f'{db_type.upper()}_HOST')
    db_port = int(os.environ.get(f'{db_type.upper()}_PORT', 0))
    ssl_ca = os.environ.get(f'{db_type.upper()}_SSL_CA')
    ssl_cert = os.environ.get(f'{db_type.upper()}_SSL_CERT')
    ssl_key = os.environ.get(f'{db_type.upper()}_SSL_KEY')
    ssl_verify_cert = bool(os.environ.get(f'{db_type.upper()}_SSL_VERIFY_CERT', False))

    if not all([db_user, db_password, db_host, db_database]):
        raise EnvironmentError(f"{db_type.upper()} environmental variables not set correctly.")

    use_tls = ssl_ca and ssl_cert and ssl_key and is_remote(db_host)

    if db_type == 'mysql' and db_port == 0:
        db_port = 3306
    elif db_type == 'postgres' and db_port == 0:
        db_port = 5432

    connection_args = {
        'user': db_user,
        'password': db_password,
        'host': db_host,
        'port': db_port,
        'database': db_database,
        'ssl_ca': ssl_ca,
        'ssl_cert': ssl_cert,
        'ssl_key': ssl_key,
        'ssl_verify_cert': ssl_verify_cert
    }

    def fetch_chunks():
        """
        Fetch data in chunks from the database table.

        Yields:
            pandas.DataFrame: A chunk of the data from the table.
        """
        offset = 0
        max_chunk_size_bytes = max_chunk_size_mb * 1024 * 1024
        estimated_row_size = None
        estimated_limit = max_chunk_rows

        spinner = Halo(text='Analyzing source table and calculating chunks...', spinner='line')
        spinner.start()

        try:
            with connect_database(db_type, use_tls, connection_args) as conn:
                with conn.cursor() as cursor:
                    while True:
                        rows = []
                        current_chunk_size = 0
                        columns = []

                        while len(rows) < max_chunk_rows and current_chunk_size < max_chunk_size_bytes:
                            fetched_rows, description = fetch_data_from_table(db_type, cursor, source_table, estimated_limit, offset)

                            if not fetched_rows:
                                break

                            if not columns and description:
                                columns = [col[0] for col in description]

                            rows.extend(fetched_rows)
                            offset += len(fetched_rows)

                            current_chunk_size = utils.total_size(rows)
                            estimated_row_size = current_chunk_size / len(rows)

                            if current_chunk_size > max_chunk_size_bytes:
                                excess_size = current_chunk_size - max_chunk_size_bytes
                                rows_to_remove = max(1,int(excess_size / estimated_row_size))

                                rows = rows[:-rows_to_remove]
                                offset -= rows_to_remove
                                current_chunk_size -= estimated_row_size * rows_to_remove
                                estimated_limit = len(rows)
                                break

                        if not rows:
                            break
                        
                        print(f'final current_chunk_size={current_chunk_size} - rows={len(rows)} - offset={offset} - limit={estimated_limit}')
                        
                        df_chunk = pd.DataFrame(rows, columns=columns)
                        yield df_chunk

                        # Adjust estimated_limit for the next iteration
                        if current_chunk_size < max_chunk_size_bytes * 0.75:
                            estimated_limit = min(max_chunk_rows, estimated_limit * 2)

            spinner.succeed('Data analysis completed!')
        except Exception as e:
            spinner.fail('Error during data analysis!')
            raise e
        except KeyboardInterrupt:
            spinner.stop()
            print("Data analysis interrupted!")

    return fetch_chunks()