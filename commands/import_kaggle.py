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
            raise ValueError("Error: Please provide a valid data file from the dataset.")
        
        if not dataset_name:
            raise ValueError("Error: Please provide a valid dataset name.")

        kaggle = ek.EdgSQLKaggle()
        
        return _import_kaggle_dataset(kaggle, dataset_name, data_file)
    except ValueError as ve:
        raise ve
    except Exception as e:
        raise RuntimeError(f"Unexpected error: {e}") from e

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
            raise ValueError(f"Error: Failed to import dataset '{dataset_name}' from Kaggle.")
        
        return kaggle.get_dataset()
    except ValueError as ve:
        raise ve
    except Exception as e:
        raise RuntimeError(f"Error importing dataset '{dataset_name}' from Kaggle: {e}") from e

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
            raise ValueError("Error: Dataset name cannot be empty.")

        if not data_name:
            raise ValueError("Error: Data name cannot be empty.")

        return import_data_kaggle(dataset, data_name)
    except ValueError as ve:
        raise ve
    except Exception as e:
        raise RuntimeError(f"Unexpected error: {e}") from e
