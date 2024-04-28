import sqlparse
import pandas as pd

def sql_to_list(sql_buffer):
    """
    Split a buffer of SQL statements into a list of individual commands.

    Args:
        sql_buffer (str): The buffer containing SQL commands.

    Returns:
        list: A list of individual SQL commands.
    """
    try:
        # Verifica se a entrada é uma string
        if not isinstance(sql_buffer, str):
            raise ValueError("Input must be a string")
        
        # Use sqlparse's split method to split SQL commands
        # This handles cases like semicolons within strings or comments
        sql_commands = sqlparse.split(sql_buffer)
        
        # Remove any leading or trailing whitespace from each command
        sql_commands = [cmd.strip() for cmd in sql_commands if cmd.strip()]
        
        return sql_commands
    except Exception as e:
        print(f"Error splitting SQL commands: {e}")
        return []

def generate_create_table_sql(df, table_name):
    """
    Generate a SQL CREATE TABLE statement based on the DataFrame structure.

    This function inspects the DataFrame's columns and data types to generate a SQL
    CREATE TABLE statement suitable for creating a table in a relational database
    like SQLite. It maps Pandas data types to their equivalent SQLite data types.

    Args:
        df (pandas.DataFrame): The DataFrame object from which to generate the SQL statement.
        table_name (str): The name of the table to be created.

    Returns:
        str: A SQL CREATE TABLE statement.
    """
    columns = []
    for column_name, dtype in df.dtypes.items():
        if dtype == 'object':
            # Check if the column contains mainly binary data
            if df[column_name].apply(lambda x: isinstance(x, bytes)).all():
                columns.append(f"{column_name} BLOB")
            else:
                columns.append(f"{column_name} TEXT")
        elif dtype == 'int64':
            columns.append(f"{column_name} INTEGER")
        elif dtype == 'float64':
            columns.append(f"{column_name} REAL")
        elif dtype == 'bool':
            columns.append(f"{column_name} INTEGER")  # Mapeia para INTEGER
        elif dtype == 'datetime64[ns]':
            columns.append(f"{column_name} TIMESTAMP")  # Map datetime to TIMESTAMP
        # Add more conditions as needed for other data types
    
    sql = f"CREATE TABLE {table_name} (\n"
    sql += ",\n".join(columns)
    sql += "\n);"
    return sql

def generate_insert_sql(df, table_name):
    """
    Generate SQL INSERT statements based on the DataFrame data.

    Args:
        df (pandas.DataFrame): The DataFrame containing data to be inserted.
        table_name (str): The name of the table into which data will be inserted.

    Returns:
        list: A list of SQL INSERT statements.
    """
    sql_commands = []
    for row in df.itertuples(index=False):
        values = []
        for value in row:
            if pd.isnull(value):
                values.append("NULL")
            elif isinstance(value, str):
                # Escape single quotes within the string by doubling them
                value = value.replace("'", "''")
                values.append(f"'{value}'")
            elif isinstance(value, pd.Timestamp):
                # Format datetime values as strings
                values.append(f"'{value}'")
            else:
                values.append(str(value))
        values_str = ", ".join(values)
        sql = f"INSERT INTO {table_name} VALUES ({values_str});"
        sql_commands.append(sql)
    return sql_commands


def format_sql(statement, reindent=True, keyword_case='upper'):
    """
    Format SQL statement using sqlparse library.

    Args:
        statement (str): The SQL statement to be formatted.
        reindent (bool, optional): Whether to reindent the SQL statement. Defaults to True.
        keyword_case (str, optional): Case of keywords after formatting. Defaults to 'upper'.

    Returns:
        str: Formatted SQL statement, or original statement if formatting fails.
    """
    try:
        if not isinstance(statement, str):
            raise ValueError("Statement must be a string")

        formatted_statement = sqlparse.format(statement, reindent=reindent, keyword_case=keyword_case)
        return formatted_statement
    except Exception as e:
        print(f"Error formatting SQL: {e}")
        return None