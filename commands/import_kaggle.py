import os
import pandas as pd
import edgesql_kaggle as ek

def import_data_kaggle(dataset_name, data_file):
    """
    Import data from a Kaggle dataset into a specified database table.

    Args:
        dataset_name (str): The name of the dataset on Kaggle to be imported.
        data_file (str): The path to the data file within the Kaggle dataset.

    Returns:
        DataFrame: The imported dataset as a pandas DataFrame.
    """
    try:
        if not data_file:
            raise Exception("Error: Please provide a valid data file from the dataset.")
        
        if not dataset_name:
            raise Exception("Error: Please provide a valid dataset name.")
        
        username = os.environ.get('KAGGLE_USERNAME')
        api_key = os.environ.get('KAGGLE_KEY')
        
        if username is None:
            raise Exception("Error: Kaggle username account not found in environment variable KAGGLE_USERNAME.")
        
        if api_key is None:
            raise Exception("Error: Kaggle API Key not found in environment variable KAGGLE_KEY.")
        
        kaggle = ek.EdgSQLKaggle(username, api_key)
        
        return _import_kaggle_dataset(kaggle, dataset_name, data_file)
    except Exception as e:
        raise Exception(f'Error importing Kaggle data: {e}')

def _import_kaggle_dataset(kaggle, dataset_name, data_file):
    """
    Import dataset from Kaggle into a specified database table in chunks.

    Args:
        kaggle (kg.EdgSQLKaggle): Initialized Kaggle API object.
        dataset_name (str): The name of the dataset on Kaggle to be imported.
        data_file (str): The path to the data file within the Kaggle dataset.

    Returns:
        DataFrame: The imported dataset as a pandas DataFrame.
    """
    try:
        import_success = kaggle.import_dataset(dataset_name, data_file)
        if not import_success:
            raise Exception(f"Error: Failed to import dataset '{dataset_name}' from Kaggle.")
        
        return kaggle.get_dataset()
    except Exception as e:
        raise Exception(f"Error importing dataset '{dataset_name}' from Kaggle: {e}")

def importer(dataset, data_name):
    """
    Import data from a Kaggle dataset into a database table.

    Args:
        dataset (str): The name of the dataset on Kaggle to be imported.
        data_name (str): The name of the data file within the Kaggle dataset.

    Returns:
        DataFrame: The imported dataset as a pandas DataFrame.
    """
    try:
        if not dataset:
            raise Exception("Error: Dataset name cannot be empty.")

        if not data_name:
            raise Exception("Error: Data name cannot be empty.")

        return import_data_kaggle(dataset, data_name)
    except Exception as e:
        raise Exception(f"Error during import: {e}")
