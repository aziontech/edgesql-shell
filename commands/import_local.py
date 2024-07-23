import os
import pandas as pd

def import_data(file_type, file_path):
    """
    Import data from a CSV or Excel file into a pandas DataFrame.

    Args:
        file_type (str): The type of file to import ('csv' or 'xlsx').
        file_path (str): The path to the file containing the data to be imported.

    Returns:
        pandas.DataFrame or None: The DataFrame containing the imported data, or None if an error occurs.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        pd.errors.EmptyDataError: If the specified file is empty or contains no data.
        pd.errors.ParserError: If an error occurs while parsing the file (e.g., incorrect format).
        ValueError: If the file format is not supported.
        Exception: For any other unexpected errors during the import process.
    """
    try:
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f'The specified file "{file_path}" does not exist.')

        if file_type not in ['csv', 'xlsx']:
            raise ValueError("Unsupported file format. Supported formats are CSV and Excel.")

        if file_type == 'csv':
            df = pd.read_csv(file_path)
        elif file_type == 'xlsx':
            df = pd.read_excel(file_path)

        return df
    except FileNotFoundError as e:
        raise FileNotFoundError(str(e)) from e
    except pd.errors.EmptyDataError as e:
        raise pd.errors.EmptyDataError(f'The specified file "{file_path}" is empty or contains no data.') from e
    except pd.errors.ParserError as e:
        raise pd.errors.ParserError(f'Error parsing "{file_path}". Please check if the file format is correct.') from e
    except ValueError as e:
        raise ValueError(str(e)) from e

def importer(file_type, file_path):
    """
    Import data from a local CSV or Excel file into a pandas DataFrame.

    Args:
        file_type (str): The type of file to import ('csv' or 'xlsx').
        file_path (str): The path to the file containing the data to be imported.

    Returns:
        pandas.DataFrame or None: The DataFrame containing the imported data, or None if an error occurs.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        pd.errors.EmptyDataError: If the specified file is empty or contains no data.
        pd.errors.ParserError: If an error occurs while parsing the file (e.g., incorrect format).
        ValueError: If the file format is not supported.
        Exception: For any other unexpected errors during the import process.
    """
    try:
        return import_data(file_type, file_path)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Error: The specified file '{file_path}' does not exist.") from e
    except pd.errors.EmptyDataError as e:
        raise pd.errors.EmptyDataError(f"The specified file '{file_path}' is empty or contains no data.") from e
    except pd.errors.ParserError as e:
        raise pd.errors.ParserError(f"Error parsing '{file_path}'. Please check the file format.") from e
    except ValueError as e:
        raise ValueError(str(e)) from e
    
