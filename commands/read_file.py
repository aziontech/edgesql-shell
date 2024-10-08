import os
from tqdm import tqdm
from halo import Halo
import utils

def fetch_sql_commands_from_file(file, limit, offset):
    """
    Fetch SQL commands from a file until the specified limit, starting from the given byte offset.
    Ignores lines containing 'BEGIN TRANSACTION;' and 'COMMIT;'.

    Args:
        file (file object): The file object opened for reading.
        limit (int): The maximum number of SQL commands to fetch.
        offset (int): The byte offset to start reading from.

    Returns:
        list: Fetched SQL commands from the file.
        int: Position in the file after reading (byte offset).
    """
    file.seek(offset)
    commands = []
    command = ""
    in_string = False

    while True:
        line_start_offset = file.tell()
        line = file.readline()
        if not line:
            break

        if line.upper() in ['BEGIN TRANSACTION;', 'COMMIT;']:
            continue

        i = 0
        while i < len(line):
            char = line[i]

            if in_string:
                if char == "'":
                    if i + 1 < len(line) and line[i + 1] == char:
                        command += "''"
                        i += 1
                    else:
                        in_string = False
                        command += char
                else:
                    command += char
            else:
                if char == "'":
                    in_string = True

                command += char

            if char == ';' and not in_string:
                commands.append(command)
                command = ""

                if len(commands) >= limit:
                    current_position = file.tell()
                    return commands, current_position

            i += 1

    current_position = file.tell()

    if command.strip():
        file.seek(line_start_offset)
        current_position = line_start_offset

    return commands, current_position

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
