import pandas as pd
import os
import zipfile
import json

if not 'KAGGLE_USERNAME' in os.environ or not 'KAGGLE_KEY' in os.environ:
    os.environ['KAGGLE_USERNAME'] = '__NONE__'
    os.environ['KAGGLE_KEY'] = '__NONE__'

import kaggle
from kaggle.api.kaggle_api_extended import Configuration
from kaggle.rest import ApiException

class EdgSQLKaggle:
    def __init__(self):
        """
        Initialize EdgSQLKaggle with Kaggle API credentials.
        """
        self._load_credentials()
        self.api = None
        self._authenticate_kaggle()
        self._df = None
        self.download_dir = "./kaggle_datasets"

        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)

    def _load_credentials(self):
        """
        Load Kaggle credentials from kaggle.json or environment variables.
        """
        kaggle_json_path = os.path.expanduser('~/.kaggle/kaggle.json')
        if os.path.isfile(kaggle_json_path):
            with open(kaggle_json_path, 'r', encoding='utf-8') as f:
                kaggle_json_data = json.load(f)
                kaggle_username = kaggle_json_data.get('username')
                kaggle_key = kaggle_json_data.get('key')

                if kaggle_username and kaggle_key:
                    os.environ['KAGGLE_USERNAME'] = kaggle_username
                    os.environ['KAGGLE_KEY'] = kaggle_key
                    self.username = kaggle_username
                    self.api_key = kaggle_key
        elif 'KAGGLE_USERNAME' in os.environ and 'KAGGLE_KEY' in os.environ:
            self.username = os.environ['KAGGLE_USERNAME']
            self.api_key = os.environ['KAGGLE_KEY']
        else:
            raise RuntimeError("Kaggle credentials not found.")

    def _authenticate_kaggle(self):
        """
        Authenticate with Kaggle API using provided credentials.
        """
        try:
            configuration = Configuration()
            configuration.api_key['username'] = self.username
            configuration.api_key['key'] = self.api_key

            self.api = kaggle.KaggleApi(kaggle.ApiClient(configuration))
            self.api.authenticate()
        except ApiException as e:
            raise RuntimeError(f'Error authenticating with Kaggle API: {e}') from e

    def get_dataset(self):
        """
        Get the dataset stored in this object.

        Returns:
            pd.DataFrame or None: The dataset if available, None otherwise.
        """
        return self._df

    def get_local_dataset_path(self, dataset_name, data_file):
        """
        Get the local file path of the dataset.

        Args:
            dataset_name (str): The name of the dataset on Kaggle.
            data_file (str): The name of the file within the dataset.

        Returns:
            str: The local file path of the dataset.
        """
        return os.path.join(self.download_dir, dataset_name.replace('/', '_'), data_file)

    def import_dataset(self, dataset_name, data_file):
        """
        Download and import a dataset from Kaggle.

        Args:
            dataset_name (str): The name of the dataset on Kaggle to be downloaded.
            data_file (str): The name of the file within the dataset to be loaded as a DataFrame.

        Returns:
            bool: True if the import is successful, False otherwise.

        Raises:
            Exception: If any error occurs during the download or import process.
        """
        if not isinstance(dataset_name, str):
            raise ValueError('Error: dataset_name must be a string.')
        if not isinstance(data_file, str):
            raise ValueError('Error: data_file must be a string.')
        if not self.api:
            raise RuntimeError('Error: Kaggle API not authenticated.')

        try:
            # Prepare directory for dataset
            dataset_dir = os.path.join(self.download_dir, dataset_name.replace('/', '_'))
            if not os.path.exists(dataset_dir):
                os.makedirs(dataset_dir)

            # Download the Kaggle dataset
            self.api.dataset_download_file(dataset_name, data_file, path=dataset_dir)
            zip_file_path = os.path.join(dataset_dir, data_file + '.zip')

            # Extract the downloaded zip file
            if zipfile.is_zipfile(zip_file_path):
                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    zip_ref.extractall(dataset_dir)
                os.remove(zip_file_path)

            local_file_path = self.get_local_dataset_path(dataset_name, data_file)
            self._df = pd.read_csv(local_file_path)
            return True
        except ApiException as e:
            raise RuntimeError(f'Error importing Kaggle dataset "{dataset_name}": {e}') from e
        except FileNotFoundError as e:
            raise FileNotFoundError(f'Error: Dataset "{dataset_name}" not found on Kaggle.') from e
        except pd.errors.EmptyDataError as e:
            raise ValueError(f'Error: Dataset "{dataset_name}" is empty or contains no data.') from e
        except pd.errors.ParserError as e:
            raise ValueError(f'Error parsing dataset "{dataset_name}": {e}') from e
        except Exception as e:
            raise RuntimeError(f'Error importing Kaggle dataset "{dataset_name}": {e}') from e
