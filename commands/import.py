import utils
import utils_sql as sql
from tqdm import tqdm
from commands import import_file as file
from commands import import_kaggle as kaggle
from commands import import_database as database
from commands import import_turso as turso

def _import_data(edgeSql, dataset_generator, table_name):
    """
    Import data into a specified database table in chunks with a progress bar.

    Args:
        edgeSql (EdgeSQL): An instance of the EdgeSQL class.
        dataset_generator (generator): A generator yielding pandas DataFrames (chunks) to be imported.
        table_name (str): The name of the database table where the data will be imported.

    Returns:
        bool: True if the import is successful, False otherwise.
    """
    try:
        total_chunks = 0
        chunks = []
        
        # Pre-read to estimate total chunks
        for chunk in dataset_generator:
            chunks.append(chunk)
            total_chunks += 1
        
        utils.write_output('Importing data...')
        progress_bar = tqdm(total=total_chunks, desc="Progress", unit="chunk", dynamic_ncols=True)

        # Import chunks
        dataset_generator = iter(chunks)
        for chunk in dataset_generator:
            if not edgeSql.exist_table(table_name):
                create_sql = sql.generate_create_table_sql(chunk, table_name)
                result = edgeSql.execute(create_sql)
                if not result['success']:
                    utils.write_output(f"Error creating table: {result['error']}")
                    return False

            # Generate SQL for data insertion if not exist
            insert_sql = sql.generate_insert_sql(chunk, table_name)
            result = edgeSql.execute(insert_sql)
            if not result['success']:
                utils.write_output(f"Error inserting data: {result['error']}")
                return False
            
            # Update progress bar
            progress_bar.update(1)

        progress_bar.close()
        
        return True
    except Exception as e:
        raise RuntimeError(f'{e}') from e
    return False

def do_import(shell, arg):
    """
    Import data from file files, Kaggle datasets, or databases into a database table.

    Args:
        arg (str): The import command and its arguments.

    Command Formats:
        .import file <csv|xlsx> <file_path> <table_name>: Import data from a file CSV or Excel file.
        .import kaggle <dataset> <data_name> <table_name>: Import data from a Kaggle dataset.
        .import mysql <database> <source_table> <table_name>: Import data from a MySQL database table.
        .import postgres <database> <source_table> <table_name>: Import data from PostgreSQL database table.
        .import sqlite <database> <source_table> <table_name>: Import data from an SQLite database table.
        .import turso <database> <source_table> <table_name>: Import data from Turso database.

    Examples:
        .import file csv /path/to/file.csv my_table
        .import kaggle joonasyoon/google-doodles list.csv list
        .import mysql mydb_name source_table_name my_table
        .import turso <database> <source_table> <table_name>
    """

    if not shell.edgeSql.get_current_database_id():
        utils.write_output("No database selected. Use '.use <database_name>' to select a database.")
        return

    args = arg.split()

    if len(args) < 4:
        utils.write_output("Usage:")
        utils.write_output(".import file <csv|xlsx> <file_path> <table_name>")
        utils.write_output(".import kaggle <dataset> <data_name> <table_name>")
        utils.write_output(".import mysql <database> <source_table> <table_name>")
        utils.write_output(".import postgres <database> <source_table> <table_name>")
        utils.write_output(".import sqlite <database> <source_table> <table_name>")
        utils.write_output(".import turso <database> <source_table> <table_name>")
        return

    sub_command = args[0]

    try:
        if sub_command == 'file':
            file_type = args[1]
            file_path = args[2]
            table_name = args[3]

            if not table_name:
                utils.write_output("Error: Table name cannot be empty.")
                return

            dataset_generator = file.importer(file_type, file_path)
        elif sub_command == 'kaggle':
            dataset = args[1]
            data_name = args[2]
            table_name = args[3]

            if not table_name:
                utils.write_output("Error: Table name cannot be empty.")
                return

            dataset_generator = kaggle.importer(dataset, data_name)
        elif sub_command in ['mysql', 'postgres']:
            db_name = args[1]
            db_table_name = args[2]
            table_name = args[3]

            if not table_name:
                utils.write_output("Error: Table name cannot be empty.")
                return

            dataset_generator = database.importer(sub_command, db_name, db_table_name)
        elif sub_command == 'sqlite':
            db_name = args[1]
            db_table_name = args[2]
            table_name = args[3]

            if not table_name:
                utils.write_output("Error: Table name cannot be empty.")
                return

            dataset_generator = database.importer(sub_command, db_name, db_table_name)
        elif sub_command == 'turso':
            db_name = args[1]
            db_table_name = args[2]
            table_name = args[3]

            if not table_name:
                utils.write_output("Error: Table name cannot be empty.")
                return

            dataset_generator = turso.importer(db_name, db_table_name)
        else:
            utils.write_output("Invalid arguments.")
            return

        status = _import_data(shell.edgeSql, dataset_generator, table_name)
        if status:
            utils.write_output(f"Data imported successfully into table '{table_name}'.")
        else:
            utils.write_output("Error: No data to import or import failed.")
    except Exception as e:
        raise RuntimeError(f"Error during import: {e}") from e
