import utils
import utils_sql as sql
from tqdm import tqdm
from commands import import_local as local
from commands import import_kaggle as kaggle
from commands import import_database as database
from commands import import_turso as turso

def _import_data(edgeSql, dataset, table_name, chunk_size=1000):
    """
    Import data into a specified database table in chunks with a progress bar.

    Args:
        edgeSql (EdgeSQL): An instance of the EdgeSQL class.
        dataset (pandas.DataFrame): The dataset to be imported.
        table_name (str): The name of the database table where the data will be imported.
        chunk_size (int, optional): Size of each data chunk for insertion. Default is 1000.

    Returns:
        bool: True if the import is successful, False otherwise.
    """
    try:
        # Check if the table exists and create it if necessary
        if not edgeSql.exist_table(table_name):
            create_sql = sql.generate_create_table_sql(dataset, table_name)
            edgeSql.execute(create_sql)

        total_chunks = len(dataset) // chunk_size + (1 if len(dataset) % chunk_size != 0 else 0)

        with tqdm(total=total_chunks, desc="Progress", unit="chunk") as progress_bar:
            for i, chunk in enumerate([dataset[i:i + chunk_size] for i in range(0, len(dataset), chunk_size)], 1):
                # Generate SQL for data insertion
                insert_sql = sql.generate_insert_sql(chunk, table_name)
                edgeSql.execute(insert_sql)

                # Update progress bar
                progress_bar.update(1)

        return True  # Import successful
    except Exception as e:
        utils.write_output(f'Error inserting data into database: {e}')
        return False

def do_import(shell, arg):
    """
    Import data from local files, Kaggle datasets, or databases into a database table.

    Args:
        arg (str): The import command and its arguments.

    Command Formats:
        .import local <csv|xlsx> <file_path> <table_name>: Import data from a local CSV or Excel file.
        .import kaggle <dataset> <data_name> <table_name>: Import data from a Kaggle dataset.
        .import database <mysql|postgresql> <source_table> <table_name>: Import data from a database table.
        .import turso <database> <source_table> <table_name>: Import data from Turso database.

    Examples:
        .import local csv /path/to/file.csv my_table
        .import kaggle joonasyoon/google-doodles list.csv list
        .import database mysql source_table_name my_table
        .import turso <database> <source_table> <table_name>
    """

    if not shell.edgeSql.get_current_database_id():
        utils.write_output("No database selected. Use '.use <database_name>' to select a database.")
        return

    args = arg.split()

    if len(args) < 4:
        utils.write_output("Usage:")
        utils.write_output(".import local <csv|xlsx> <file_path> <table_name>")
        utils.write_output(".import kaggle <dataset> <data_name> <table_name>")
        utils.write_output(".import mysql <database> <source_table> <table_name>")
        utils.write_output(".import postgresql <database> <source_table> <table_name>")
        utils.write_output(".import turso <database> <source_table> <table_name>")
        return

    sub_command = args[0]

    try:
        if sub_command == 'local':
            file_type = args[1]
            file_path = args[2]
            table_name = args[3]

            if not table_name:
                utils.write_output("Error: Table name cannot be empty.")
                return

            df = local.importer(file_type, file_path)
        elif sub_command == 'kaggle':    
            dataset = args[1]
            data_name = args[2]
            table_name = args[3]

            if not table_name:
                utils.write_output("Error: Table name cannot be empty.")
                return

            df = kaggle.importer(dataset, data_name)
        elif sub_command in ['mysql','postgres']:
            db_name = args[1]
            db_table_name = args[2]
            table_name = args[3]

            if not table_name:
                utils.write_output("Error: Table name cannot be empty.")
                return

            df = database.importer(sub_command, db_name, db_table_name)
        elif sub_command == 'turso':
            db_name = args[1]
            db_table_name = args[2]
            table_name = args[3]

            if not table_name:
                utils.write_output("Error: Table name cannot be empty.")
                return

            df = turso.importer(db_name, db_table_name)
        else:
            utils.write_output("Invalid arguments.")
            return

        if df is not None and not df.empty:
            _import_data(shell.edgeSql, df, table_name)
            utils.write_output(f"Data imported successfully into table '{table_name}'.")
        else:
            utils.write_output("Error: No data to import or import failed.")
    except Exception as e:
        utils.write_output(f"Error during import: {e}")
