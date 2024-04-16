import utils
import utils_sql as sql
import requests
from http import HTTPStatus

BASE_URL = 'https://api.azion.com/v4/edge_sql/schemas'


class EdgeSQL:
    def __init__(self, token, base_url=None):
        """
        Initialize EdgeSQL object with the provided API token.

        Args:
            token (str): The API token for authentication.
        """
        self._token = token
        self._current_database_id = None
        self._current_database_name = None
        self._base_url = base_url if base_url is not None else BASE_URL

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
            dict or None: The result of the SQL execution, including columns and rows if successful, or None if failed.
        """
        output = None
        if self._current_database_id is None:
            utils.write_output("No database selected. Use '.use <database_name>' to select a database.")
            return

        if not buffer:
            utils.write_output("No SQL commands provided.")
            return

        if isinstance(buffer, list):
            sql_commands = buffer
        elif isinstance(buffer, str):
            sql_commands = sql.sql_to_list(buffer)
        else:
            utils.write_output("Invalid SQL commands format.")
            return

        url = f'{self._base_url}/{self._current_database_id}/execute'
        data = {"statements": sql_commands}
        self.transaction = False

        try:
            response = requests.post(url, json=data, headers=self.__headers())
            json_data = response.json()
            json_data = response.json()

            if response.status_code == HTTPStatus.OK:  # 200
                result_data = json_data.get('data', [])
                if result_data:
                    result = result_data[0]
                    if 'error' in result:
                        utils.write_output(f'"Error:", {result.get("error")}')
                    else:
                        results = result.get('results', {})
                        columns = results.get('columns', [])
                        rows = results.get('rows', [])
                        output = {'columns': columns, 'rows': rows}
                else:
                    utils.write_output("Error: Empty or invalid response data.")
            else:
                msg_err = json_data.get('error', 'Unknown error')
                utils.write_output(f'{msg_err}')
        except requests.RequestException as e:
            utils.write_output(f'Error: {e}')

        return output

    def list_databases(self):
        """
        List all databases available.

        Returns:
            str or None: A formatted table containing database information if successful, or None if failed.
        """
        try:
            response = requests.get(self._base_url, headers=self.__headers())
            response.raise_for_status()
            json_data = response.json()

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
                    utils.write_output("No databases found.")
            else:
                msg_err = json_data.get('detail', 'Unknown error')
                utils.write_output(f'Error: {msg_err}')
        except requests.RequestException as e:
            utils.write_output(f'Error: {e}')

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
            response = requests.get(self._base_url, headers=self.__headers())
            response.raise_for_status()
            json_data = response.json()
        
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
                    utils.write_output("No databases found.")
            else:
                msg_err = json_data.get('detail', 'Unknown error')
                utils.write_output(f'Error: {msg_err}')
        except requests.RequestException as e:
            utils.write_output(f'Error: {e}')

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
            response = requests.get(self._base_url, headers=self.__headers())
            response.raise_for_status()
            json_data = response.json()

            if response.status_code == HTTPStatus.OK:  # 200
                databases = json_data.get('results')
                if databases:
                    for db in json_data['results']:
                        if db.get('name') == database_name:
                            return db.get('id')
                    utils.write_output(f"Database '{database_name}' not found.")
                else:
                    utils.write_output("No databases found.")
            else:
                msg_err = json_data.get('detail', 'Unknown error')
                utils.write_output(f'Error: {msg_err}')
        except requests.RequestException as e:
            utils.write_output(f'Error: {e}')

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
            response = requests.get(url, headers=self.__headers())
            response.raise_for_status()
            json_data = response.json()

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
                utils.write_output(f'Error: {msg_err}')
        except requests.RequestException as e:
            utils.write_output(f'Error: {e}')

        return database_info

    def create_database(self, database_name):
        """
        Create a new database with the given name.

        Args:
            database_name (str): The name of the new database.

        Returns:
            None
        """
        if not database_name:
            utils.write_output("Usage: create_database <database_name>")
            return

        data = {"name": database_name}

        try:
            response = requests.post(self._base_url, json=data, headers=self.__headers())
            response.raise_for_status()
            json_data = response.json()

            if response.status_code == HTTPStatus.ACCEPTED or response.status_code == HTTPStatus.CREATED:  # 201/202
                data = json_data.get('data')
                if data:
                    db_id = data.get('id')
                    db_name = data.get('name')
                    if db_id is not None and db_name is not None:
                        utils.write_output(f"New database created. ID: {db_id}, Name: {db_name}")
                        return
                    else:
                        utils.write_output("Error: Unexpected response format.")
                else:
                    utils.write_output("Error: Unexpected response format.")
            else:
                error_detail = json_data.get('detail', 'Unknown error')
                error_name = json_data.get('name', '')
                if error_detail:
                    utils.write_output(f"Error: {error_detail}")
                elif error_name:
                    utils.write_output(f"Error: {error_name[0]}")
                else:
                    utils.write_output(f"Error: {response.status_code}")
        except requests.RequestException as e:
            utils.write_output(f'Error: {e}')

    def destroy_database(self, database_name):
        """
        Destroy a database by name.

        Args:
            database_name (str): The name of the database to destroy.

        Returns:
            None
        """
        if not database_name:
            utils.write_output("Usage: destroy_database <database_name>")
            return

        database_id = self.get_database_id(database_name)
        if not database_id:
            utils.write_output(f"Database '{database_name}' not found.")
            return

        url = f'{self._base_url}/{database_id}'

        try:
            response = requests.delete(url, headers=self.__headers())
            response.raise_for_status()

            if response.status_code == HTTPStatus.ACCEPTED:  # 202
                if self._current_database_name == database_name:
                    self._current_database_id = None
                    self._current_database_name = None
                utils.write_output('Database deleted successfully.')
            else:
                msg_err = response.json().get('detail', 'Unknown error')
                utils.write_output(f'Error: {msg_err}')
        except requests.RequestException as e:
            utils.write_output(f'Error: {e}')

    def list_tables(self):
        """
        List all tables in the current database.

        Returns:
            dict or None: A dictionary containing table columns and rows if successful, or None if failed.
        """
        return self.execute("PRAGMA table_list;")

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

        return self.execute(f"PRAGMA table_info({table_name});")

    def exist_table(self, table_name):
        """
        Check if a table exists in the current database.

        Args:
            table_name (str): The name of the table to check.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        buffer = self.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        if buffer:
            return True
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

