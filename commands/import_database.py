import os
import pandas as pd
import mysql.connector
import psycopg2

def is_remote(host):
    # Modify this function based on your criteria to detect local or remote connections
    # Here, we assume localhost or 127.0.0.1 as local, and any other host as remote
    return host not in ['localhost', '127.0.0.1']

def connect_database(db_type, use_tls, connection_args):
    if db_type == 'mysql':
        return connect_mysql(use_tls, connection_args)
    elif db_type == 'postgres':
        return connect_postgres(use_tls, connection_args)
    else:
        raise Exception("Invalid database type. Use 'mysql' or 'postgres'.")

def connect_mysql(use_tls, connection_args):
    if use_tls:
        return mysql.connector.connect(**connection_args)
    else:
        return mysql.connector.connect(
            user=connection_args['user'],
            password=connection_args['password'],
            host=connection_args['host'],
            port=connection_args['port'],  # Default MySQL port is 3306
            database=connection_args['database']
        )

def connect_postgres(use_tls, connection_args):
    if use_tls:
        return psycopg2.connect(**connection_args)
    else:
        return psycopg2.connect(
            user=connection_args['user'],
            password=connection_args['password'],
            host=connection_args['host'],
            port=connection_args['port'],  # Default PostgreSQL port is 5432
            database=connection_args['database']
        )

def fetch_data_from_table(cursor, source_table):
    query = f"SELECT * FROM {source_table}"
    cursor.execute(query)
    return cursor.fetchall()

def importer(db_type, db_database, source_table):
    """
    Import data from a database table into a Pandas DataFrame.

    Args:
        db_type (str): The type of the database ('mysql' or 'postgres').
        source_table (str): The name of the source table in the database.

    Returns:
        pd.DataFrame: DataFrame containing the imported data.

    Raises:
        Exception: If there is an error during the import process or if the database type is invalid.
    """
    # Get database credentials and SSL parameters from environmental variables
    db_user = os.environ.get(f'{db_type.upper()}_USERNAME')
    db_password = os.environ.get(f'{db_type.upper()}_PASSWORD')
    db_host = os.environ.get(f'{db_type.upper()}_HOST')
    db_port = int(os.environ.get(f'{db_type.upper()}_PORT', 0))  # Default port set to 0 initially
    ssl_ca = os.environ.get(f'{db_type.upper()}_SSL_CA')
    ssl_cert = os.environ.get(f'{db_type.upper()}_SSL_CERT')
    ssl_key = os.environ.get(f'{db_type.upper()}_SSL_KEY')
    ssl_verify_cert = bool(os.environ.get(f'{db_type.upper()}_SSL_VERIFY_CERT', False))

    if not all([db_user, db_password, db_host, db_database]):
        raise Exception(f"{db_type.upper()} environmental variables not set correctly.")

    # Determine if TLS should be used based on the connection type and SSL parameters
    if ssl_ca is None or ssl_cert is None or ssl_key is None or is_remote(db_host) == False:
        use_tls = False
    else:
        use_tls = True

    # Set default ports for MySQL and PostgreSQL if not provided in the environment variables
    if db_type == 'mysql' and db_port == 0:
        db_port = 3306  # Default MySQL port is 3306
    elif db_type == 'postgres' and db_port == 0:
        db_port = 5432  # Default PostgreSQL port is 5432

    # Establish a connection using TLS only for remote connections
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

    try:
        with connect_database(db_type, use_tls, connection_args) as conn:
            with conn.cursor() as cursor:
                rows = fetch_data_from_table(cursor, source_table)

                # Check if cursor.description is a tuple of tuples
                if cursor.description is not None:
                    # Extract column names from cursor.description
                    columns = [col[0] for col in cursor.description]
                else:
                    raise Exception("Unable to fetch column names from cursor description.")

        # Import data into a Pandas DataFrame
        return pd.DataFrame(rows, columns=columns)

    except Exception as e:
        raise Exception(f"Error during {db_type} import: {e}")
