#from kaggle.api.kaggle_api_extended import KaggleApi
import kaggle
from kaggle.api.kaggle_api_extended import Configuration
from kaggle.rest import ApiException
import pandas as pd
import utils
import os
import zipfile

class EdgSQLKaggle:
    def __init__(self, username, api_key):
        """
        Initialize EdgSQLKaggle with Kaggle API credentials.

        Args:
            username (str): Kaggle username.
            api_key (str): Kaggle API key.
        """

        self.username = username
        self.api_key = api_key
        self.api = None
        self._authenticate_kaggle()

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
            #self.api.datasets_list()

            return True
        except ApiException as e:
            utils.write_output(f'Error authenticating with Kaggle API: {e}')
            return False

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
            utils.write_output('Error: dataset_name must be strings.')
            return False

        if not self.api:
            utils.write_output('Error: Kaggle API not authenticated.')
            return False

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
            utils.write_output(f'Error importing Kaggle dataset "{dataset_name}": {e}')
            return False
        except FileNotFoundError:
            utils.write_output(f'Error: Dataset "{dataset_name}" not found on Kaggle.')
            return False
        except pd.errors.EmptyDataError:
            utils.write_output(f'Error: Dataset "{dataset_name}" is empty or contains no data.')
            return False
        except pd.errors.ParserError as e:
            utils.write_output(f'Error parsing dataset "{dataset_name}": {e}')
            return False
        except Exception as e:
            utils.write_output(f'Error importing Kaggle dataset "{dataset_name}": {e}')
            return False
