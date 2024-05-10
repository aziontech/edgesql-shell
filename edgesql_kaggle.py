#from kaggle.api.kaggle_api_extended import KaggleApi
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

        Args:
            username (str): Kaggle username.
            api_key (str): Kaggle API key.
        """

        self._load_credentials()
        self.api = None
        self._authenticate_kaggle()

    def _load_credentials(self):
        # Check if kaggle.json file is available
        kaggle_json_path = os.path.expanduser('~/.kaggle/kaggle.json')
        if os.path.isfile(kaggle_json_path):
            with open(kaggle_json_path, 'r') as f:
                kaggle_json_data = json.load(f)
                kaggle_username = kaggle_json_data.get('username')
                kaggle_key = kaggle_json_data.get('key')

                if kaggle_username and kaggle_key:
                    # Set the environment variables
                    os.environ['KAGGLE_USERNAME'] = kaggle_username
                    os.environ['KAGGLE_KEY'] = kaggle_key
                    self.username = kaggle_username
                    self.api_key = kaggle_key
        elif os.environ['KAGGLE_USERNAME'] != '__NONE__' \
            and os.environ['KAGGLE_KEY'] != '__NONE__':
            self.username = os.environ.get('KAGGLE_USERNAME')
            self.api_key = os.environ.get('KAGGLE_KEY')
        else:
            raise Exception("Kaggle credentials not found.")

    def _authenticate_kaggle(self):
        """
        Authenticate with Kaggle API using provided credentials.

        Returns:
            bool: True if authentication is successful, False otherwise.
        """
        try:
            configuration = Configuration()
            configuration.api_key['username'] = self.username
            configuration.api_key['key'] = self.api_key

            self.api = kaggle.KaggleApi(kaggle.ApiClient(configuration))
            self.api.authenticate()

            return True
        except ApiException as e:
            raise Exception(f'Error authenticating with Kaggle API: {e}')

    def get_dataset(self):
        """
        Get the dataset stored in this object.

        Returns:
            pd.DataFrame or None: The dataset if available, None otherwise.
        """
        return self._df if hasattr(self, '_df') else None

    def import_dataset(self, dataset_name, data_file):
        """
        Download and import a dataset from Kaggle.

        Args:
            dataset_name (str): The name of the dataset on Kaggle to be downloaded.

        Returns:
            bool: True if the import is successful, False otherwise.

        Raises:
            Exception: If any error occurs during the download or import process.
        """

        if not isinstance(dataset_name, str):
            raise Exception('Error: dataset_name must be strings.')

        if not self.api:
            raise Exception('Error: Kaggle API not authenticated.')

        try:
            # Download the Kaggle dataset
            self.api.dataset_download_file(dataset_name, data_file)

            # Check if the downloaded file is a zip file
            zip_file = data_file+'.zip'
            if zipfile.is_zipfile(zip_file):
                with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                    zip_ref.extractall('.')  # Extract the contents of the zip file
                os.remove(zip_file)  # Remove the downloaded zip file

            self._df = pd.read_csv(data_file)

            # Delete temporary files
            os.remove(data_file)

            return True  # Indicating successful import
        except ApiException as e:
            raise Exception(f'Error importing Kaggle dataset "{dataset_name}": {e}')
        except FileNotFoundError:
            raise Exception(f'Error: Dataset "{dataset_name}" not found on Kaggle.')
        except pd.errors.EmptyDataError:
            raise Exception(f'Error: Dataset "{dataset_name}" is empty or contains no data.')
        except pd.errors.ParserError as e:
            raise Exception(f'Error parsing dataset "{dataset_name}": {e}')
        except Exception as e:
            raise Exception(f'Error importing Kaggle dataset "{dataset_name}": {e}')
