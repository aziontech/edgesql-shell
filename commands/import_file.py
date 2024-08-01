import os
import pandas as pd
from halo import Halo

def get_size_of_chunk(df):
    return df.memory_usage(index=True, deep=True).sum()

def import_data(file_type, file_path, max_chunk_size_mb=0.8, chunksize=512):
    """
    Import data from a CSV or Excel file in chunks, with an adaptive chunk size to avoid exceeding memory limits.

    Args:
        file_type (str): The type of file to import ('csv' or 'xlsx').
        file_path (str): The path to the file containing the data to be imported.
        max_chunk_size_mb (float, optional): The maximum size of each chunk in MB. Default is 0.8 MB.
        chunksize (int, optional): The initial number of rows per chunk to read at a time (for CSV files).

    Yields:
        pandas.DataFrame: A chunk of the data from the file.
    """
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f'The specified file "{file_path}" does not exist.')

    if file_type not in ['csv', 'xlsx']:
        raise ValueError("Unsupported file format. Supported formats are CSV and Excel.")

    max_chunk_size_bytes = max_chunk_size_mb * 1024 * 1024

    spinner = Halo(text='Analyzing source table and calculating chunks...', spinner='line')
    spinner.start()

    try:
        if file_type == 'csv':
            for chunk in pd.read_csv(file_path, chunksize=chunksize):
                current_chunk_size = get_size_of_chunk(chunk)
                if current_chunk_size > max_chunk_size_bytes:
                    rows = chunk.shape[0]
                    while current_chunk_size > max_chunk_size_bytes and rows > 0:
                        chunk = chunk.iloc[:-1]
                        current_chunk_size = get_size_of_chunk(chunk)
                        rows = chunk.shape[0]
                yield chunk
        elif file_type == 'xlsx':
            excel_file = pd.ExcelFile(file_path)
            for sheet in excel_file.sheet_names:
                sheet_data = pd.read_excel(file_path, sheet_name=sheet, chunksize=chunksize)
                for chunk in sheet_data:
                    current_chunk_size = get_size_of_chunk(chunk)
                    if current_chunk_size > max_chunk_size_bytes:
                        rows = chunk.shape[0]
                        while current_chunk_size > max_chunk_size_bytes and rows > 0:
                            chunk = chunk.iloc[:-1]
                            current_chunk_size = get_size_of_chunk(chunk)
                            rows = chunk.shape[0]
                    yield chunk
    except pd.errors.EmptyDataError as er:
        raise pd.errors.EmptyDataError(f'The specified file "{file_path}" is empty or contains no data.') from er
    except pd.errors.ParserError as er:
        raise pd.errors.ParserError(f'Error parsing "{file_path}". Please check if the file format is correct.') from er
    except RuntimeError as er:
        raise RuntimeError(f"An unexpected error occurred while importing the file: {er}") from er
    finally:
        spinner.succeed('Data analysis completed!')

def importer(file_type, file_path, max_chunk_size_mb=0.8, chunksize=512):
    """
    Wrapper to import data in chunks from a file (CSV or Excel).

    Args:
        file_type (str): The type of file to import ('csv' or 'xlsx').
        file_path (str): The path to the file containing the data to be imported.
        max_chunk_size_mb (float, optional): The maximum size of each chunk in MB. Default is 2.5 MB.
        chunksize (int, optional): Size of each data chunk for insertion. Default is 512.

    Yields:
        pandas.DataFrame: A chunk of the data from the file.
    """
    return import_data(file_type, file_path, max_chunk_size_mb, chunksize)
