import utils
import utils_sql as sql
import requests
from http import HTTPStatus
import json

BASE_URL = 'https://api.azion.com/v4/edge_sql/databases'


class EdgeSQL:
    def __init__(self, token, base_url=None):
        """
        Initialize EdgeSQL object with the provided API token.

        Args:
            token (str): The API token for authentication.
        """
        self.timeout=120
        self._token = token
        self._current_database_id = None
        self._current_database_name = None
        self._base_url = base_url if base_url is not None else BASE_URL
        self.transaction = False

    @property
    def token(self):
        """
        Get the API token.

        Returns:
            str: The API token.
        """
        return self._token

    @token.setter
    def token(self, token):
        """
        Set the API token.

        Args:
            token (str): The API token to set.
        """
        self._token = token

    def get_current_database_id(self):
        """
        Get the ID of the current selected database.

        Returns:
            str: The ID of the current selected database.
        """
        return self._current_database_id

    def get_current_database_name(self):
        """
        Get the name of the current selected database.

        Returns:
            str: The name of the current selected database.
        """
        return self._current_database_name

    def execute(self, buffer):
        """
        Execute SQL commands on the currently selected database.

        Args:
            buffer (str or list): The SQL commands to execute, either as a string or a list of strings.

        Returns:
            dict: A dictionary containing 'success' (bool) and 'data' (dict or None) or 'error' (str).
        """
        result = {'success': False, 'data': None, 'error': None}

        # Check if a database is selected
        if self._current_database_id is None:
            error_msg = "No database selected. Use '.use <database_name>' to select a database."
            result['error'] = error_msg
            return result

        # Check if buffer is provided
        if not buffer:
            error_msg = "No SQL commands provided."
            result['error'] = error_msg
            return result

        # Convert buffer to list of SQL commands
        if isinstance(buffer, list):
            sql_commands = buffer
        elif isinstance(buffer, str):
            sql_commands = sql.sql_to_list(buffer)
        else:
            error_msg = "Invalid SQL commands format."
            result['error'] = error_msg
            return result

        # Prepare the request
        url = f'{self._base_url}/{self._current_database_id}/query'
        data = {"statements": sql_commands}
        self.transaction = False

        try:
            response = requests.post(url, json=data, headers=self.__headers(), timeout=self.timeout)

            # Check if the response content is empty
            if not response.content:
                result['error'] = f"Empty response from server. statusCode={response.status_code}"
                return result

            try:
                json_data = response.json()
            except json.JSONDecodeError as e:
                result['error'] = f"Error decoding JSON response: {e}. statusCode={response.status_code}"
                return result

            if response.status_code == HTTPStatus.OK:
                result_data = json_data.get('data', [])
                if result_data:
                    query_result = result_data[0]
                    if 'error' in query_result:
                        error_msg = f"{query_result.get('error')}"
                        result['error'] = error_msg
                    else:
                        results = query_result.get('results', {})
                        columns = results.get('columns', [])
                        rows = results.get('rows', [])
                        result['data'] = {'columns': columns, 'rows': rows}
                        result['success'] = True
                else:
                    error_msg = "Empty or invalid response data."
                    result['error'] = error_msg
            else:
                error_msg = json_data.get('error', 'Unknown error')
                result['error'] = error_msg
        except requests.RequestException as e:
            error_msg = f"Request error: {e}"
            result['error'] = error_msg

        return result


    def list_databases(self):
        """
        List all databases available.

        Returns:
            str or None: A formatted table containing database information if successful, or None if failed.
        """
        try:
            response = requests.get(self._base_url, headers=self.__headers(), timeout=self.timeout)
            try:
                json_data = response.json()
            except json.JSONDecodeError as e:
                raise ValueError(f"Error decoding JSON response: {e}. statusCode={response.status_code}") from e

            if response.status_code == HTTPStatus.OK:  # 200
                databases = json_data.get('results')
                if databases:
                    db_list = {
                        'databases': [
                            (db.get('id'), db.get('name'), db.get('status'), db.get('created_at'), db.get('updated_at'))
                            for db in databases
                        ],
                        'columns': ['ID', 'Name', 'Status', 'Created At', 'Updated At']
                    }
                    return db_list
            else:
                msg_err = json_data.get('detail', 'Unknown error')
                raise ValueError(f'{msg_err}')
        except requests.RequestException as e:
            raise requests.RequestException(f'{e}') from e

        return None

    def set_current_database(self, database_name):
        """
        Set the current selected database by name.

        Args:
            database_name (str): The name of the database to select.

        Returns:
            bool: True if the database was successfully selected, False otherwise.
        """
        try:
            response = requests.get(self._base_url, headers=self.__headers(), timeout=self.timeout)
            try:
                json_data = response.json()
            except json.JSONDecodeError as e:
                raise ValueError(f"Error decoding JSON response: {e}. statusCode={response.status_code}") from e

            if response.status_code == HTTPStatus.OK:  # 200
                databases = json_data.get('results')
                if databases:
                    for db in json_data['results']:
                        if db['name'] == database_name:
                            self._current_database_id = db['id']
                            self._current_database_name = db['name']
                            return True
                    utils.write_output(f"Database '{database_name}' not found.")
            else:
                msg_err = json_data.get('detail', 'Unknown error')
                raise ValueError(f'{msg_err}')
        except requests.RequestException as e:
            raise requests.RequestException(f'{e}') from e

        return False

    def get_database_id(self, database_name):
        """
        Get the ID of a database by name.

        Args:
            database_name (str): The name of the database.

        Returns:
            str: The ID of the database if found, or -1 if not found.
        """
        try:
            response = requests.get(self._base_url, headers=self.__headers(), timeout=self.timeout)
            try:
                json_data = response.json()
            except json.JSONDecodeError as e:
                raise ValueError(f"Error decoding JSON response: {e}. statusCode={response.status_code}") from e

            if response.status_code == HTTPStatus.OK:  # 200
                databases = json_data.get('results',[])
                if databases:
                    for db in databases: #json_data['results']:
                        if db.get('name') == database_name:
                            return db.get('id')
            else:
                msg_err = json_data.get('detail', 'Unknown error')
                raise ValueError(f'{msg_err}')
        except requests.RequestException as e:
            raise requests.RequestException(f'{e}') from e

        return None

    def get_database_info(self):
        """
        Get information about the currently selected database.

        Returns:
            str or None: A formatted table containing database information if successful, or None if failed.
        """
        database_info = None
        if self._current_database_id is None:
            utils.write_output("No database selected. Use '.use <database_name>' to select a database.")
            return None

        url = f'{self._base_url}/{self._current_database_id}'
        try:
            response = requests.get(url, headers=self.__headers(), timeout=self.timeout)
            try:
                json_data = response.json()
            except json.JSONDecodeError as e:
                raise ValueError(f"Error decoding JSON response: {e}. statusCode={response.status_code}") from e

            if response.status_code == HTTPStatus.OK:  # 200
                data = json_data.get('data')
                if data:
                    table_data = [
                        ["Database ID", data['id']],
                        ["Database Name", data['name']],
                        ["Client ID", data['client_id']],
                        ["Status", data['status']],
                        ["Created At", data['created_at']],
                        ["Updated At", data['updated_at']]
                    ]
                    database_info = {'table_data':table_data, 'columns': ["Attribute", "Value"]}
                else:
                    utils.write_output("Error: Database information not found in response.")
            else:
                msg_err = json_data.get('detail', 'Unknown error')
                raise ValueError(f'{msg_err}')
        except requests.RequestException as e:
            raise requests.RequestException(f'{e}') from e

        return database_info
    
    def get_database_size(self):
        """
        Get size of the currently selected database in MB.

        Returns:
            str or None: A formatted table containing database information if successful, or None if failed.
        """
        query = 'SELECT (page_count * page_size) / 1024.0 / 1024.0 AS size_in_mb \
                    FROM ( \
                        SELECT (SELECT page_count FROM pragma_page_count) AS page_count, \
                        (SELECT page_size FROM pragma_page_size) AS page_size \
                );'
        if self._current_database_id is None:
            utils.write_output("No database selected. Use '.use <database_name>' to select a database.")
            return None

        try:
            return self.execute(query)
        except Exception as e:
            raise RuntimeError(f'{e}') from e

    def create_database(self, database_name):
        """
        Create a new database with the given name.

        Args:
            database_name (str): The name of the new database.

        Returns:
            bool: True if the database was successfully created, False otherwise.
        """
        if not database_name:
            utils.write_output("Usage: create_database <database_name>")
            return False

        data = {"name": database_name}

        try:
            response = requests.post(self._base_url, json=data, headers=self.__headers(), timeout=self.timeout)
            try:
                json_data = response.json()
            except json.JSONDecodeError as e:
                raise ValueError(f"Error decoding JSON response: {e}. statusCode={response.status_code}") from e

            if response.status_code in [HTTPStatus.ACCEPTED, HTTPStatus.CREATED]:  # 201/202
                data = json_data.get('data')
                if data:
                    db_id = data.get('id')
                    db_name = data.get('name')
                    if db_id is not None and db_name is not None:
                        utils.write_output(f"New database created. ID: {db_id}, Name: {db_name}")
                        return True
                utils.write_output("Unexpected response format.")
                return False
            else:
                error_detail = json_data.get('detail', 'Unknown error')
                error_name = json_data.get('name', '')
                if error_detail:
                    utils.write_output(f"Error creating database: {error_detail}")
                elif error_name:
                    utils.write_output(f"Error creating database: {error_name[0]}")
                else:
                    utils.write_output(f"Error creating database: {response.status_code}")
                return False
        except requests.RequestException as e:
            utils.write_output(f"Error creating database: {e}")
            return False

    def destroy_database(self, database_name):
        """
        Destroy a database by name.

        Args:
            database_name (str): The name of the database to destroy.

        Returns:
            bool: True if the database was successfully destroyed, False otherwise.
        """
        if not database_name:
            utils.write_output("Usage: destroy_database <database_name>")
            return False

        database_id = self.get_database_id(database_name)
        if not database_id:
            utils.write_output(f"Database '{database_name}' not found.")
            return False

        url = f'{self._base_url}/{database_id}'

        try:
            response = requests.delete(url, headers=self.__headers(), timeout=self.timeout)

            if response.status_code == HTTPStatus.ACCEPTED:  # 202
                if self._current_database_name == database_name:
                    self._current_database_id = None
                    self._current_database_name = None
                return True
            else:
                msg_err = response.json().get('detail', 'Unknown error')
                utils.write_output(f"Error deleting database: {msg_err}")
                return False
        except requests.RequestException as e:
            raise requests.RequestException(f"Error deleting database: {e}") from e

    def list_tables(self):
        """
        List all tables in the current database.

        Returns:
            dict or None: A dictionary containing table columns and rows if successful, or None if failed.
        """
        try:
            return self.execute("PRAGMA table_list;")
        except Exception as e:
            raise RuntimeError(f'{e}') from e

    def describe_table(self, table_name):
        """
        Describe the structure of a table.

        Args:
            table_name (str): The name of the table to describe.

        Returns:
            dict or None: A dictionary containing table information if successful, or None if failed.
        """
        if not table_name:
            utils.write_output("Usage: describe_table <table_name>")
            return None

        try:
            return self.execute(f"PRAGMA table_info({table_name});")
        except Exception as e:
            raise RuntimeError(f'{e}') from e

    def exist_table(self, table_name):
        """
        Check if a table exists in the current database.

        Args:
            table_name (str): The name of the table to check.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
        try:
            result = self.execute(query)
            if result['success'] == False:
                return False
        except Exception as e:
            raise RuntimeError(f'{e}') from e

        data = result.get('data', {})
        if 'rows' in data and len(data['rows']) > 0:
            return True
        else:
            return False

    def __headers(self):
        """
        Get the request headers including authorization token.

        Returns:
            dict: The request headers.
        """
        return {
            'accept': 'application/json',
            'Authorization': f'Token {self._token}',
            'Content-Type': 'application/json'
        }
