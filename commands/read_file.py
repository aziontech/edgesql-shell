import utils
import os
from pathlib import Path

def do_read(shell, arg):
    """
    Load SQL statements from file and execute them.

    Args:
        arg (str): File name.
    """
    if not arg:
        utils.write_output("Usage: .read <file_name>")
        return

    file_name = arg
    try:
        if Path(file_name).is_file():
            read_sql_from_file(shell.edgeSql, file_name)
            utils.write_output(f"SQL statements from {file_name} executed successfully.")
        else:
            utils.write_output(f"Error: File '{file_name}' not found.")
    except Exception as e:
        utils.write_output(f"Error during execution: {e}")


def read_sql_from_file(edgeSql, file_name):
    """Read SQL statements from a file and execute them."""
    if not os.path.isfile(file_name):
        utils.write_output(f"File '{file_name}' not found.")
        return

    try:
        with open(file_name, 'r') as file:
            sql_statements = file.read()
    except Exception as e:
        utils.write_output(f"Error reading file '{file_name}': {e}")
        return

    try:
        edgeSql.execute(sql_statements)
    except Exception as e:
        utils.write_output(f'Error executing SQL statements from file: {e}')