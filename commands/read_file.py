import utils
import os
from pathlib import Path
from tqdm import tqdm

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
    except FileNotFoundError as e:
        raise FileNotFoundError(f"File not found during execution: {e}")
    except IOError as e:
        raise IOError(f"I/O error during execution: {e}")
    except Exception as e:
        raise Exception(f"Unexpected error during execution: {e}")


def read_sql_from_file(edgeSql, file_name, chunk_size=512):
    """Read SQL statements from a file and execute them in chunks."""
    
    if not os.path.isfile(file_name):
        utils.write_output(f"File '{file_name}' not found.")
        return

    try:
        with open(file_name, 'r', encoding='utf-8') as file:
            sql_statements = file.read().strip().split(';')
            # Remove any empty statements that may result from splitting
            sql_statements = [stmt.strip() for stmt in sql_statements if stmt.strip()]
    except FileNotFoundError as e:
        raise FileNotFoundError(f"File '{file_name}' not found.") from e
    except IOError as e:
        raise IOError(f"Error reading file '{file_name}': {e}") from e

    total_chunks = len(sql_statements) // chunk_size + (1 if len(sql_statements) % chunk_size != 0 else 0)

    try:
        # Execute SQL statements in chunks
        with tqdm(total=total_chunks, desc="Progress", unit="chunk") as progress_bar:
            for i in range(0, len(sql_statements), chunk_size):
                chunk = sql_statements[i:i + chunk_size]
                try:
                    # Join SQL statements with ';' as separator and execute
                    sql_chunk = ';'.join(chunk) + ';'
                    edgeSql.execute(sql_chunk)
                except Exception as e:
                    raise Exception(f'Error executing SQL chunk: {e}')

                # Update progress bar
                progress_bar.update(1)
                
        return True  # Execution successful
    except Exception as e:
        raise Exception(f'Error processing SQL file: {e}')
