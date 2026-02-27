import os
from tqdm import tqdm
from halo import Halo
import utils

def fetch_sql_commands_from_file(file, limit, offset):
    """
    Fetch SQL commands from a file using sqlparse for robust parsing.
    Uses command-based chunking instead of byte-based to avoid cutting commands in half.

    Args:
        file (file object): The file object opened for reading.
        limit (int): The maximum number of SQL commands to fetch.
        offset (int): The byte offset to start reading from.

    Returns:
        list: Fetched SQL commands from the file.
        int: Position in the file after reading (byte offset).
    """
    import utils_sql as sql
    
    # If this is the first call (offset = 0), parse the entire file once
    # and store commands in a global cache to avoid re-parsing
    if not hasattr(fetch_sql_commands_from_file, '_cached_commands'):
        file.seek(0)
        content = file.read()
        
        # Filter out transaction control statements
        lines = content.split('\n')
        filtered_lines = []
        
        for line in lines:
            stripped = line.strip().upper()
            if stripped not in ['BEGIN TRANSACTION;', 'COMMIT;', 'BEGIN;', 'COMMIT']:
                filtered_lines.append(line)
        
        filtered_content = '\n'.join(filtered_lines)
        
        # Use sqlparse for robust parsing (same as execute() method)
        try:
            all_commands = sql.sql_to_list(filtered_content)
            fetch_sql_commands_from_file._cached_commands = all_commands
            fetch_sql_commands_from_file._command_index = 0
        except Exception as e:
            # Fallback to simple splitting if sqlparse fails
            simple_commands = [cmd.strip() for cmd in filtered_content.split(';') if cmd.strip()]
            fetch_sql_commands_from_file._cached_commands = simple_commands
            fetch_sql_commands_from_file._command_index = 0
    
    # Return the next batch of commands
    start_idx = fetch_sql_commands_from_file._command_index
    end_idx = start_idx + limit if limit else len(fetch_sql_commands_from_file._cached_commands)
    
    commands = fetch_sql_commands_from_file._cached_commands[start_idx:end_idx]
    
    # Update index for next call
    fetch_sql_commands_from_file._command_index = end_idx
    
    # Calculate new position (approximate)
    if commands:
        new_position = offset + sum(len(cmd.encode('utf-8')) for cmd in commands)
    else:
        new_position = offset
    
    return commands, new_position

def limit_estimation(rows, max_chunk_size_bytes, margin):
    chunk_size = int(utils.total_size(rows) // len(rows))
    effective_max_chunk_size = margin * max_chunk_size_bytes
    num_entries = int(effective_max_chunk_size // chunk_size)
    
    return num_entries

def fetch_chunks(file_name, max_chunk_rows=512, max_chunk_size_mb=0.8):
    """
    Fetch data in chunks from a file.

    Yields:
        list: A chunk of the data.
    """
    byte_offset = 0
    max_chunk_size_bytes = max_chunk_size_mb * 1024 * 1024
    estimated_limit = 1

    spinner = Halo(text='Analyzing source file and calculating chunks...', spinner='line')
    spinner.start()

    try:
        with open(file_name, 'r', encoding='utf-8') as file:
            while True:
                rows = []
                current_chunk_size = 0
                limit_reached = False

                while len(rows) < estimated_limit and current_chunk_size < max_chunk_size_bytes:
                    fetched_rows, new_byte_offset = fetch_sql_commands_from_file(file, estimated_limit, byte_offset)

                    if not fetched_rows:
                        break

                    partial_chunk_size = 0
                    partial_rows = []
                    for row in fetched_rows:
                        row_size = utils.total_size(row)
                        if partial_chunk_size + row_size > max_chunk_size_bytes:
                            #reset offset and try again with lower chunk size
                            new_byte_offset = byte_offset
                            estimated_limit = len(partial_rows)
                            partial_rows.clear()
                            partial_chunk_size = 0
                            limit_reached = True
                            break

                        partial_rows.append(row)
                        partial_chunk_size += row_size

                    rows.extend(partial_rows)
                    current_chunk_size += partial_chunk_size
                    byte_offset = new_byte_offset              

                if not rows:
                    break

                yield rows

                if limit_reached:
                    estimated_limit = max(1, len(rows))
                else:
                    candiate_limit = min(limit_estimation(rows, max_chunk_size_bytes, 0.85), max_chunk_rows)
                    if candiate_limit > estimated_limit:
                        estimated_limit = candiate_limit

        spinner.succeed('Data analysis completed!')
    except Exception as e:
        spinner.fail('Error during data analysis!')
        raise e
    except KeyboardInterrupt:
        spinner.stop()
        print("Data analysis interrupted!")


def _import_data(edgeSql, dataset_generator, file_name):
    if not os.path.isfile(file_name):
        utils.write_output(f"File '{file_name}' not found.")
        return False

    try:
        total_chunks = 0
        chunks = []
        for chunk in dataset_generator:
            chunks.append(chunk)
            total_chunks += 1

        utils.write_output('Importing data...')
        progress_bar = tqdm(total=total_chunks, desc="Progress", unit="chunk", dynamic_ncols=True)

        dataset_generator = iter(chunks)
        for chunk in dataset_generator:
            try:
                result = edgeSql.execute(chunk)
                if not result['success']:
                    utils.write_output(f"Error executing SQL chunk: {result['error']}")
                    return False

                progress_bar.update(1)
            except RuntimeError as e:
                utils.write_output(f"Error executing SQL: {e}\nFrom command {result['command']}")
                return False

        progress_bar.close()
        return True
    except (FileNotFoundError, IOError, RuntimeError) as e:
        utils.write_output(f"Error during import: {e}")
        return False
    except Exception as e:
        utils.write_output(f"Critical error during import: {e}")
        raise RuntimeError(f"Failed to import data from {file_name}") from e


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
    if not os.path.isfile(file_name):
        utils.write_output(f"Error: File '{file_name}' not found.")
        return

    try:
        dataset_generator = fetch_chunks(file_name)
        if _import_data(shell.edgeSql, dataset_generator, file_name):
            utils.write_output(f"SQL statements from {file_name} executed successfully.")
        else:
            utils.write_output(f"Error: Failed to execute SQL statements from {file_name}.")
    except FileNotFoundError:
        utils.write_output(f"File '{file_name}' not found during execution.")
    except IOError as e:
        utils.write_output(f"I/O error during execution: {e}")
    except RuntimeError as e:
        utils.write_output(f"Runtime error during execution: {e}")
    except Exception as e:
        utils.write_output(f"An unexpected error occurred during execution: {e}")
        raise RuntimeError(f"Unexpected error during execution of {file_name}") from e
