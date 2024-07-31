import sqlparse
import pandas as pd
import numpy as np
import json

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
    except ValueError as e:
        print(f"ValueError splitting SQL commands: {e}")
        return []
    except RuntimeError as e:
        print(f"Unexpected error splitting SQL commands: {e}")
        return []
    
def identify_vector_columns(df, sample_size=1000):
    vector_columns = {}
    
    for column in df.columns:
        if df[column].dtype == 'object':
            # Sample the column to speed up the process for large datasets
            sample = df[column].dropna().head(sample_size)
            
            if len(sample) > 0:
                first_val = sample.iloc[0]
                
                if isinstance(first_val, str):
                    try:
                        parsed = eval(first_val)
                        if isinstance(parsed, (list, np.ndarray)) and all(isinstance(x, (int, float)) for x in parsed):
                            vector_columns[column] = len(parsed)
                    except:
                        pass
                elif isinstance(first_val, (list, np.ndarray)):
                    if all(isinstance(x, (int, float)) for x in first_val):
                        vector_columns[column] = len(first_val)
    
    return vector_columns

def is_json(value):
    if not isinstance(value, str):
        return False
    try:
        json.loads(value)
        return True
    except json.JSONDecodeError:
        return False

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
    # Replace spaces with underscores in column names
    df.columns = df.columns.str.replace(' ', '_').str.replace('.', '_')

    # identify_vector_columns
    vector_columns = identify_vector_columns(df)
    print(df.dtypes)

    columns = []
    for column_name, dtype in df.dtypes.items():
        if column_name in vector_columns:
            columns.append(f"{column_name} F32_BLOB({vector_columns[column_name]})")
        elif dtype == 'object':
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
            columns.append(f"{column_name} INTEGER")  # Map bool to INTEGER
        elif dtype == 'datetime64[ns]' or dtype == 'datetime64[ns, UTC]':
            columns.append(f"{column_name} TIMESTAMP")  # Map datetime to TIMESTAMP
        else:
            columns.append(f"{column_name} TEXT")  # Default to TEXT for any other type

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
    vector_columns = identify_vector_columns(df)
    column_names = df.columns.tolist()

    for row in df.itertuples(index=False):
        values = []
        for column_name, value in zip(column_names, row):
            vector_name = None
            print(column_name)
            if column_name in vector_columns:
                values.append(f"vector('{value}')")
                print("is vector")
            elif pd.isnull(value):
                values.append("NULL")
                print("is null")
            elif isinstance(value, str):
                # Escape single quotes within the string by doubling them
                value = value.replace("'", "''")
                values.append(f"'{value}'")
                print("is string")
            elif isinstance(value, pd.Timestamp):
                # Format datetime values as strings
                values.append(f"'{value}'")
                print("is timestamp")
            elif is_json(str(value).replace('\'','\"')):
                values.append(f"'{str(value).replace('\'','\"')}'")
                print("is json")
            else:
                values.append(str(value))
                print("is something else")

        values_str = ", ".join(values)
        sql = f"INSERT INTO {table_name} VALUES ({values_str});"
        sql_commands.append(sql)
    print(sql_commands)
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
    except ValueError as e:
        raise ValueError(f"ValueError formatting SQL: {e}") from e
    except Exception as e:
        raise ValueError(f"Unexpected error formatting SQL: {e}") from e
    