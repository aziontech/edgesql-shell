import sqlparse
import pandas as pd
import json
import numpy as np
import ast
import re

def sql_to_list(sql_buffer):
    """
    Split a buffer of SQL statements into a list of individual commands.

    Args:
        sql_buffer (str): The buffer containing SQL commands.

    Returns:
        list: A list of individual SQL commands.
    """
    try:
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
    """
    Identify columns in the DataFrame that contain vectors (lists or arrays of numbers).

    Args:
        df (pandas.DataFrame): The DataFrame to inspect.
        sample_size (int): The number of rows to sample for detecting vector columns.

    Returns:
        dict: A dictionary where the keys are column names and the values are the length of the vectors.
    """
    vector_columns = {}

    for column in df.columns:
        if df[column].dtype == 'object':
            sample = df[column].dropna().head(sample_size)
            if len(sample) > 0:
                first_val = sample.iloc[0]
                if isinstance(first_val, str):
                    try:
                        parsed = ast.literal_eval(first_val)
                        if isinstance(parsed, (list, np.ndarray)) and all(isinstance(x, (int, float)) for x in parsed):
                            vector_columns[column] = len(parsed)
                    except (ValueError, SyntaxError):
                        pass
                elif isinstance(first_val, (list, np.ndarray)):
                    if all(isinstance(x, (int, float)) for x in first_val):
                        vector_columns[column] = len(first_val)
    return vector_columns

def is_json(value):
    """
    Check if a given string value is a valid JSON.

    Args:
        value (str): The string to check.

    Returns:
        bool: True if the string is valid JSON, False otherwise.
    """
    if not isinstance(value, str):
        return False
    try:
        json.loads(value)
        return True
    except json.JSONDecodeError:
        return False

def sanitize_value(value):
    """
    Sanitize a value for safe inclusion in SQL statements.

    Args:
        value: The value to sanitize (can be str, dict, or other types).

    Returns:
        str: The sanitized value as a string.
    """
    if isinstance(value, str):
        return value.replace("'", "''").replace("\\", "\\\\")
    elif isinstance(value, dict):
        return json.dumps(value).replace("'", "''").replace("\\", "\\\\")
    else:
        return str(value).replace("'", "''").replace("\\", "\\\\")

def generate_create_table_sql(df, table_name):
    """
    Generate a SQL CREATE TABLE statement based on the DataFrame structure.

    Args:
        df (pandas.DataFrame): The DataFrame object from which to generate the SQL statement.
        table_name (str): The name of the table to be created.

    Returns:
        str: A SQL CREATE TABLE statement.
    """
    # Replace spaces with underscores in column names
    df.columns = df.columns.str.replace(' ', '_').str.replace('.', '_')
    vector_columns = identify_vector_columns(df)

    columns = []
    for column_name, dtype in df.dtypes.items():
        if column_name in vector_columns:
            columns.append(f"{column_name} F32_BLOB({vector_columns[column_name]})")
        elif dtype == 'object':
            if df[column_name].apply(lambda x: isinstance(x, bytes)).all():
                columns.append(f"{column_name} BLOB")
            else:
                columns.append(f"{column_name} TEXT")
        elif dtype == 'int64':
            columns.append(f"{column_name} INTEGER")
        elif dtype == 'float64':
            columns.append(f"{column_name} REAL")
        elif dtype == 'bool':
            columns.append(f"{column_name} INTEGER")
        elif dtype == 'datetime64[ns]' or dtype == 'datetime64[ns, UTC]':
            columns.append(f"{column_name} TIMESTAMP")
        else:
            columns.append(f"{column_name} TEXT")

    sql = f"CREATE TABLE {table_name} (\n"
    sql += ",\n".join(columns)
    sql += "\n);"
    return sql


def generate_insert_sql(df, table_name, raw_columns=None, exclude_columns=None):
    """
    Generate SQL INSERT statements based on the DataFrame data.

    Args:
        df (pandas.DataFrame): The DataFrame containing data to be inserted.
        table_name (str): The name of the table into which data will be inserted.
        raw_columns (list, optional): List of column names whose values are raw SQL expressions.
                                       These values will not be quoted or modified. Defaults to None.
        exclude_columns (list, optional): List of column names to exclude from the INSERT. Defaults to None.

    Returns:
        list: A list of SQL INSERT statements.
    """
    if raw_columns is None:
        raw_columns = []
    if exclude_columns is None:
        exclude_columns = []

    # Exclude specific columns
    df = df.drop(columns=exclude_columns, errors='ignore')

    sql_commands = []
    df.columns = df.columns.str.replace(' ', '_').str.replace('.', '_')
    vector_columns = identify_vector_columns(df)
    column_names = df.columns.tolist()
    columns = ", ".join(column_names)
    
    for row in df.itertuples(index=False):
        values = []
        for column_name, value in zip(column_names, row):
            if column_name in raw_columns:
                if pd.isnull(value):
                    values.append("NULL")
                else:
                    values.append(f"{value}")
            elif pd.isnull(value):
                values.append("NULL")
            elif isinstance(value, str):
                sanitized_value = sanitize_value(value)
                values.append(f"'{sanitized_value}'")
            elif isinstance(value, pd.Timestamp):
                values.append(f"'{value}'")
            elif isinstance(value, (int, float)):
                values.append(str(value))
            elif isinstance(value, bool):
                values.append('1' if value else '0')
            elif isinstance(value, dict):
                sanitized_value = sanitize_value(value)
                values.append(f"'{sanitized_value}'")
            else:
                sanitized_value = sanitize_value(value)
                values.append(f"'{sanitized_value}'")
        values_str = ", ".join(values)
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values_str});"
        sql_commands.append(sql)
    return sql_commands

def format_sql(statement, reindent=True, indent_width=4, keyword_case='upper'):
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

        formatted_statement = sqlparse.format(statement, 
                                              reindent=reindent,
                                              reindent_aligned=True,
                                              indent_columns=True,
                                              indent_width=indent_width, 
                                              keyword_case=keyword_case)
        return formatted_statement
    except ValueError as e:
        raise ValueError(f"ValueError formatting SQL: {e}") from e
    except Exception as e:
        raise ValueError(f"Unexpected error formatting SQL: {e}") from e
    

def get_autoincrement_columns(create_table_sql):
    """
    Parse the CREATE TABLE statement and return a list of column names with AUTOINCREMENT.

    Args:
        create_table_sql (str): The CREATE TABLE SQL statement.

    Returns:
        list: List of column names that have AUTOINCREMENT.
    """
    autoinc_columns = []
    # Use regex to find columns with AUTOINCREMENT
    # Assume syntax: column_name TYPE PRIMARY KEY AUTOINCREMENT
    pattern = re.compile(r'\b(\w+)\s+\w+\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b', re.IGNORECASE)
    matches = pattern.findall(create_table_sql)
    autoinc_columns.extend(matches)
    return autoinc_columns

