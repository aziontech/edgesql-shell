import utils
import os
from pathlib import Path
from tqdm import tqdm

def do_read(shell, arg):
    """
    Load SQL statements from a file and execute them.

    Args:
        arg (str): File name.
    """
    if not arg:
        utils.write_output("Usage: .read <file_name>")
        return

    file_name = arg
    try:
        if Path(file_name).is_file():
            if read_sql_from_file(shell.edgeSql, file_name):
                utils.write_output(f"SQL statements from {file_name} executed successfully.")
            else:
                utils.write_output(f"Error: Failed to execute SQL statements from {file_name}.")
        else:
            utils.write_output(f"Error: File '{file_name}' not found.")
    except FileNotFoundError:
        utils.write_output(f"File '{file_name}' not found during execution.")
    except IOError as e:
        utils.write_output(f"I/O error during execution: {e}")
    except RuntimeError as e:
        utils.write_output(f"Unexpected error during execution: {e}")

def read_sql_from_file(edgeSql, file_name, chunk_size=512):
    """
    Read SQL statements from a file and execute them in chunks.

    Args:
        edgeSql (EdgeSQL): An instance of the EdgeSQL class.
        file_name (str): The name of the file containing SQL statements.
        chunk_size (int, optional): Number of SQL statements to execute per chunk. Default is 512.
    """
    if not os.path.isfile(file_name):
        utils.write_output(f"File '{file_name}' not found.")
        return False

    try:
        with open(file_name, 'r', encoding='utf-8') as file:
            sql_statements = file.read().strip().split(';')
            # Remove any empty statements that may result from splitting
            sql_statements = [stmt.strip() for stmt in sql_statements if stmt.strip()]
    except FileNotFoundError:
        utils.write_output(f"File '{file_name}' not found.")
        return False
    except IOError as e:
        utils.write_output(f"Error reading file '{file_name}': {e}")
        return False

    total_chunks = len(sql_statements) // chunk_size + (1 if len(sql_statements) % chunk_size != 0 else 0)

    try:
        # Execute SQL statements in chunks
        with tqdm(total=total_chunks, desc="Progress", unit="chunk") as progress_bar:
            for i in range(0, len(sql_statements), chunk_size):
                chunk = sql_statements[i:i + chunk_size]
                try:
                    # Join SQL statements with ';' as separator and execute
                    sql_chunk = ';'.join(chunk) + ';'
                    result = edgeSql.execute(sql_chunk)
                    if not result['success']:
                        utils.write_output(f"Error executing SQL chunk: {result['error']}")
                        return False
                except RuntimeError as e:
                    utils.write_output(f"Error executing SQL chunk: {e}")
                    return False

                # Update progress bar
                progress_bar.update(1)
                
        return True  # Execution successful
    except RuntimeError as e:
        utils.write_output(f"Error processing SQL file: {e}")
        return False
