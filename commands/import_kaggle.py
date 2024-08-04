import pandas as pd
import edgesql_kaggle as ek
from halo import Halo
import utils

def import_data_kaggle(dataset_name, data_file, max_chunk_rows=512, max_chunk_size_mb=0.8):
    """
    Import data from a Kaggle dataset in chunks, with an adaptive chunk size to avoid exceeding memory limits.

    Args:
        dataset_name (str): The name of the dataset on Kaggle to be imported.
        data_file (str): The path to the data file within the Kaggle dataset.
        max_chunk_rows (int, optional): The maximum number of rows per chunk to return. Default is 512.
        max_chunk_size_mb (float, optional): The maximum size of each chunk in megabytes. Default is 0.8 MB.

    Yields:
        pandas.DataFrame: A chunk of the data from the dataset.
    """
    try:
        if not data_file:
            raise ValueError("Error: Please provide a valid data file from the dataset.")
        
        if not dataset_name:
            raise ValueError("Error: Please provide a valid dataset name.")

        kaggle = ek.EdgSQLKaggle()
        kaggle.import_dataset(dataset_name, data_file)

        local_file_path = kaggle.get_local_dataset_path(dataset_name, data_file)
        max_chunk_size_bytes = max_chunk_size_mb * 1024 * 1024
        estimated_row_size = None
        estimated_limit = max_chunk_rows

        spinner = Halo(text='Analyzing dataset and calculating chunks...', spinner='line')
        spinner.start()

        chunk_rows = []
        current_chunk_size = 0

        for chunk in pd.read_csv(local_file_path, chunksize=1):
            row_size = utils.get_size_of_chunk(chunk)
            
            if estimated_row_size is None:
                estimated_row_size = row_size
            
            current_chunk_size += row_size
            chunk_rows.append(chunk.iloc[0])

            if current_chunk_size > max_chunk_size_bytes or len(chunk_rows) >= max_chunk_rows:
                yield pd.DataFrame(chunk_rows)
                chunk_rows = []
                current_chunk_size = 0
                estimated_limit = max(1, estimated_limit // 2)

            if current_chunk_size < max_chunk_size_bytes * 0.75:
                estimated_limit = min(max_chunk_rows, estimated_limit * 1.5)

        if chunk_rows:
            yield pd.DataFrame(chunk_rows)

    except ValueError as ve:
        raise ve
    except Exception as e:
        raise RuntimeError(f"Unexpected error: {e}") from e

    spinner.succeed('Data analysis completed!')

def importer(dataset, data_name, max_chunk_rows=512, max_chunk_size_mb=2.5):
    """
    Import data from a Kaggle dataset in chunks.

    Args:
        dataset (str): The name of the dataset on Kaggle to be imported.
        data_name (str): The name of the data file within the Kaggle dataset.
        max_chunk_rows (int, optional): Maximum number of rows per chunk. Default is 512.
        max_chunk_size_mb (float, optional): Maximum size of each chunk in megabytes. Default is 2.5 MB.

    Yields:
        pandas.DataFrame: A chunk of the data from the dataset.
    """
    return import_data_kaggle(dataset, data_name, max_chunk_rows, max_chunk_size_mb)
