import os
import utils
import utils_sql as sql
import pandas as pd
import edgesql_kaggle as ek
from tqdm import tqdm


def _import_local(shell, args):
    """
    Import data from a local CSV or Excel file into a database table.

    Args:
        shell (EdgeSQLShell): The EdgeSQLShell instance.
        args (list): A list containing the command arguments:
                     [command, sub_command, file_type, file_path, table_name].

    Command Format:
        .import local <csv|xlsx> <file_path> <table_name>

    Examples:
        .import local csv /path/to/file.csv my_table
        .import local xlsx /path/to/file.xlsx my_table
    """
    if len(args) < 4:
        utils.write_output("Usage: .import local <csv|xlsx> <file_path> <table_name>")
        return

    file_type = args[1]
    file_path = args[2]
    table_name = args[3]

    if file_type not in ['csv', 'xlsx']:
        utils.write_output("Invalid file type. Use 'csv' or 'xlsx'.")
        return

    if not file_path:
        utils.write_output("Error: File path cannot be empty.")
        return

    if not table_name:
        utils.write_output("Error: Table name cannot be empty.")
        return

    try:
        import_data(shell.edgeSql, file_type, file_path, table_name)
        utils.write_output(f"Data imported from {file_path} to table {table_name} successfully.")
    except FileNotFoundError as e:
        utils.write_output(f"Error: The specified file '{file_path}' does not exist.")
    except pd.errors.EmptyDataError:
        utils.write_output(f"The specified file '{file_path}' is empty or contains no data.")
    except pd.errors.ParserError:
        utils.write_output(f"Error parsing '{file_path}'. Please check the file format.")
    except Exception as e:
        utils.write_output(f"Error during import: {e}")


def _import_kaggle(shell, args):
    """
    Import data from a Kaggle dataset into a database table.

    Args:
        shell (EdgeSQLShell): The EdgeSQLShell instance.
        args (list): A list containing the command arguments:
                     [command, sub_command, dataset, data_name, table_name].

    Command Format:
        .import kaggle <dataset> <data_name> <table_name>

    Examples:
        .import kaggle dataset_name data_file_name my_table
    """
    if len(args) < 4:
        utils.write_output("Usage: .import kaggle <dataset> <data_name> <table_name>")
        return

    dataset = args[1]
    data_name = args[2]
    table_name = args[3]

    if not dataset:
        utils.write_output("Error: Dataset name cannot be empty.")
        return

    if not data_name:
        utils.write_output("Error: Data name cannot be empty.")
        return

    if not table_name:
        utils.write_output("Error: Table name cannot be empty.")
        return

    try:
        if import_data_kaggle(shell.edgeSql, dataset, data_name, table_name):
            utils.write_output(f"Dataset {dataset} - {data_name} imported from Kaggle to table {table_name} successfully.")
    except Exception as e:
        utils.write_output(f"Error during import: {e}")


def import_data(edgeSql, file_type, file_path, table_name, chunk_size=1000):
    """
    Import data from a file into a database table.

    Args:
        edgeSql (EdgeSQL): An instance of the EdgeSQL class.
        file_type (str): The type of file to import ('csv' or 'xlsx').
        file_path (str): The path to the file containing the data to be imported.
        table_name (str): The name of the database table where the data will be imported.
        chunk_size (int, optional): Size of each data chunk for insertion. Default is 1000.

    Returns:
        None

    Raises:
        FileNotFoundError: If the specified file does not exist.
        pd.errors.EmptyDataError: If the specified file is empty or contains no data.
        pd.errors.ParserError: If an error occurs while parsing the file (e.g., incorrect format).
        ValueError: If the file format is not supported.

    Note:
        This function supports importing data from CSV and Excel files.
    """
    try:
        # Validate file existence
        if not os.path.isfile(file_path):
            utils.write_output(f'Error: The specified file "{file_path}" does not exist.')
            return
        
        # Validate table name
        if not table_name:
            utils.write_output("Error: Table name cannot be empty.")
            return

        # Validate file type
        if file_type not in ['csv', 'xlsx']:
            raise ValueError("Unsupported file format. Supported formats are CSV and Excel.")

        # Read data from file
        if file_type == 'csv':
            df = pd.read_csv(file_path)
        elif file_type == 'xlsx':
            df = pd.read_excel(file_path)

        _import_data(edgeSql, df, table_name, chunk_size)
    except FileNotFoundError as e:
        utils.write_output(str(e))
    except pd.errors.EmptyDataError:
        utils.write_output(f'The specified file "{file_path}" is empty or contains no data.')
    except pd.errors.ParserError:
        utils.write_output(f'An error occurred while parsing "{file_path}". Please check if the file format is correct.')
    except ValueError as e:
        utils.write_output(str(e))
    except Exception as e:
        utils.write_output(f'Error importing data: {e}')


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

def import_data_kaggle(edgeSql, dataset_name, data_file, table_name, chunk_size=1000):
    """
    Import data from a Kaggle dataset into a specified database table.

    Args:
        edgeSql (EdgeSQL): An instance of the EdgeSQL class.
        dataset_name (str): The name of the dataset on Kaggle to be imported.
        data_file (str): The path to the data file within the Kaggle dataset.
        table_name (str): The name of the database table where the data will be imported.
        chunk_size (int, optional): Size of each data chunk for insertion. Default is 1000.

    Returns:
        bool: True if the import is successful, False otherwise.
    """
    try:
        # Validate table name, data file, and dataset name
        if not table_name:
            utils.write_output("Error: Please provide a valid table name.")
            return False
        
        if not data_file:
            utils.write_output("Error: Please provide a valid data file from the dataset.")
            return False
        
        if not dataset_name:
            utils.write_output("Error: Please provide a valid dataset name.")
            return False
        
        # Check environment variables for Kaggle credentials
        username = os.environ.get('KAGGLE_USERNAME')
        api_key = os.environ.get('KAGGLE_KEY')
        
        if username is None:
            utils.write_output("Error: Kaggle username account not found in environment variable KAGGLE_USERNAME.")
            return False
        
        if api_key is None:
            utils.write_output("Error: Kaggle API Key not found in environment variable KAGGLE_KEY.")
            return False
        
        # Initialize Kaggle API
        kaggle = ek.EdgSQLKaggle(username, api_key)
        
        # Import dataset from Kaggle
        import_success = _import_kaggle_dataset(edgeSql, kaggle, dataset_name, data_file, table_name, chunk_size)
        
        if import_success:
            utils.write_output(f"Dataset '{dataset_name}' imported from Kaggle to table '{table_name}' successfully.")
            return True
        else:
            utils.write_output(f"Error: Failed to import dataset '{dataset_name}' from Kaggle.")
            return False
    
    except Exception as e:
        utils.write_output(f'Error importing Kaggle data: {e}')
        return False  # Import failed



def _import_kaggle_dataset(edgeSql, kaggle, dataset_name, data_file, table_name, chunk_size):
    """
    Import dataset from Kaggle into a specified database table in chunks.

    Args:
        edgeSql (EdgeSQL): An instance of the EdgeSQL class.
        kaggle (kg.EdgSQLKaggle): Initialized Kaggle API object.
        dataset_name (str): The name of the dataset on Kaggle to be imported.
        data_file (str): The path to the data file within the Kaggle dataset.
        table_name (str): The name of the database table where the data will be imported.
        chunk_size (int): Size of each data chunk for insertion.

    Returns:
        bool: True if the import is successful, False otherwise.
    """
    try:
        # Import dataset from Kaggle
        import_success = kaggle.import_dataset(dataset_name, data_file)
        if not import_success:
            utils.write_output(f"Error: Failed to import dataset '{dataset_name}' from Kaggle.")
            return False
        
        # Get the imported dataset
        dataset = kaggle.get_dataset()
        if dataset is None:
            utils.write_output("Error: No dataset found.")
            return False

        # Perform the actual import into the database table
        return _import_data(edgeSql, dataset, table_name, chunk_size)
    
    except Exception as e:
        utils.write_output(f"Error importing dataset '{dataset_name}' from Kaggle: {e}")
        return False


def do_import(shell, arg):
    """
    Import data from local files or Kaggle datasets into a database table.

    Args:
        arg (str): The import command and its arguments.

    Command Formats:
        .import local <csv|xlsx> <file_path> <table_name>: Import data from a local CSV or Excel file.
        .import kaggle <dataset> <data_name> <table_name>: Import data from a Kaggle dataset.

    Examples:
        .import local csv /path/to/file.csv my_table
        .import kaggle dataset_name data_file_name my_table
    """
    subcommands = {
        "local": _import_local,
        "kaggle": _import_kaggle,
    }

    if not shell.edgeSql.get_current_database_id():
        utils.write_output("No database selected. Use '.use <database_name>' to select a database.")
        return
    
    args = arg.split()

    if len(args) < 4:
        utils.write_output("Usage:")
        utils.write_output(".import local <csv|xlsx> <file_path> <table_name>")
        utils.write_output(".import kaggle <dataset> <data_name> <table_name>")
        return

    sub_command = args[0]

    if sub_command in subcommands:
        try:
            subcommands[sub_command](shell, args)
        except Exception as e:
            utils.write_output(f"Error during import: {e}")
    else:
        utils.write_output("Invalid import subcommand.")