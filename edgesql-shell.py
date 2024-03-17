import os
import requests
import cmd
from tabulate import tabulate
import signal
import sqlparse

BASE_URL = 'https://api.azion.com/v4/edge_sql/schemas'

class AzionDBShell(cmd.Cmd):
    def __init__(self, token):
        super().__init__()
        self.token = token
        self.current_database_id = None
        self.current_database_name = None
        self.prompt = 'EdgeSQL'
        self.update_prompt()
        self.last_command = ''
        self.multiline_command = []

    def update_prompt(self):
        if self.current_database_name:
            self.prompt = f'EdgeSQL ({self.current_database_name})> '
        else:
            self.prompt = 'EdgeSQL> '

    def do_exit(self, arg):
        """Exit the shell."""
        print("Exiting EdgeSQL Shell.")
        return True

    def do_tables(self, arg):
        """List all tables."""
        self.execute_sql_command("PRAGMA table_list;")

    def do_schema(self, arg):
        """Describe table."""
        if not arg:
            print("Usage: .schema <table_name>")
            return
        self.execute_sql_command(f"PRAGMA table_info({arg});")

    def do_databases(self, arg):
        """List all databases."""
        self.execute_database_command()

    def do_use(self, arg):
        """Switch to a database by name."""
        if not arg:
            print("Usage: use <database_name>")
            return
        self.set_current_database(arg)

    def do_dbinfo(self, arg):
        """Get information about the current database."""
        self.get_database_info()

    def do_read(self, arg):
        """Load SQL statements from file and execute them."""
        if not arg:
            print("Usage: read <file_name>")
            return
        self.read_sql_from_file(arg)

    def execute_sql_command_multiline(self):
        """Execute a multiline command command"""
        sql_command = ' '.join(self.multiline_command)
        try:
            self.execute_sql_command(sql_command)
        except Exception as e:
             print(f"Error executing SQL command: {e}")
        self.multiline_command = []  # Reset multiline command buffer

    def default(self, arg):
        """Execute SQL command and handle multiline input."""
        if arg.strip():
            # New command
            self.last_command = arg
            if arg.startswith("."):
                command, *args = arg.split(" ")
                if command == ".exit":
                    return self.do_exit(args)
                elif command == ".tables":
                    return self.do_tables(args)
                elif command == ".schema":
                    return self.do_schema(" ".join(args))
                elif command == ".databases":
                    return self.do_databases(args)
                elif command == ".use":
                    return self.do_use(" ".join(args))
                elif command == ".dbinfo":
                    return self.do_dbinfo(args)
                elif command == ".create":
                    return self.do_create(args)
                elif command == ".destroy":
                    return self.do_destroy(args)
                elif command == ".read":
                    return self.do_read(" ".join(args))
                else:
                    print("Invalid command.")
            elif self.multiline_command:
                if arg.startswith('--'): #ignore comments
                    pass
                if any('begin' in cmd.lower() for cmd in self.multiline_command):
                    # Multi-line command with 'begin', accumulate lines
                    self.multiline_command.append(arg)
                    if 'end;' in arg.lower():
                        self.execute_sql_command_multiline()
                else:
                    self.multiline_command.append(arg)
                    if arg.endswith(';'):
                        self.execute_sql_command_multiline()
            else:
                if arg.endswith(';'):
                    # Single-line command, execute immediately
                    self.execute_sql_command(arg)    
                elif arg.startswith('--'): #ignore comments
                    pass
                else:
                    # Multi-line command, accumulate lines
                    self.multiline_command.append(arg)
        else:
            pass


    def execute_sql_command(self, buffer):
        if self.current_database_id is None:
            print("No database selected. Use '.use <database_name>' to select a database.")
            return

        url = f'{BASE_URL}/{self.current_database_id}/execute'
        headers = {
            'accept': 'application/json',
            'Authorization': f'Token {self.token}',
            'Content-Type': 'application/json'
        }

        sql_commands = self.split_sql_buffer(buffer)

        data = {"statements": sql_commands}
        response = requests.post(url, json=data, headers=headers)

        if response.status_code == 200:
            json_data = response.json()
            if 'data' in json_data and isinstance(json_data['data'], list) and json_data['data']:
                result_data = json_data['data'][0]
                if 'error' in result_data:
                    print("Error:", result_data['error'])
                else:
                    results = result_data.get('results', {})
                    columns = results.get('columns', [])
                    rows = results.get('rows', [])
                    if columns and rows:
                        formatted_table = tabulate(rows, headers=columns, tablefmt="fancy_grid")
                        print(formatted_table)
                    #else:
                    #    print("No data returned.")
            else:
                print("Error: Empty or invalid response data.")
        else:
            print("Error:", response.status_code)

    def split_sql_buffer(self, sql_buffer):
        """
        Split a buffer of SQL commands into a list of individual commands.

        Args:
            sql_buffer (str): The buffer containing SQL commands.

        Returns:
            list: A list of individual SQL commands.
        """

        # Use sqlparse's split method to split SQL commands
        # This handles cases like semicolons within strings or comments
        sql_commands = sqlparse.split(sql_buffer)

        # Remove any leading or trailing whitespace from each command
        sql_commands = [cmd.strip() for cmd in sql_commands if cmd.strip()]

        return sql_commands


    def execute_database_command(self):
        url = f'{BASE_URL}'
        headers = {
            'accept': 'application/json',
            'Authorization': f'Token {self.token}'
        }
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            json_data = response.json()
            databases = [(db['id'], db['name'], db['status'], db['created_at'], db['updated_at']) for db in json_data['results']]
            formatted_table = tabulate(databases, headers=['ID', 'Name', 'Status', 'Created At', 'Updated At'], tablefmt="fancy_grid")
            print(formatted_table)
        else:
            print("Error:", response.status_code)

    def set_current_database(self, database_name):
        url = f'{BASE_URL}'
        headers = {
            'accept': 'application/json',
            'Authorization': f'Token {self.token}'
        }
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            json_data = response.json()
            for db in json_data['results']:
                if db['name'] == database_name:
                    self.current_database_id = db['id']
                    self.current_database_name = db['name']
                    self.update_prompt()
                    print(f"Switched to database '{database_name}'.")
                    return
            print(f"Database '{database_name}' not found.")
        else:
            print("Error:", response.status_code)

    def get_database_id(self, database_name):
        # Define the URL for databases
        url = f'{BASE_URL}'

        # Define the headers
        headers = {
            'accept': 'application/json',
            'Authorization': f'Token {self.token}'
        }

        # Execute the GET request
        response = requests.get(url, headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            # Extract the JSON data from the response
            json_data = response.json()
            
            # Check if the 'results' key exists and it's not empty
            if 'results' in json_data and json_data['results']:
                # Iterate over the results to find the database ID by name
                for db in json_data['results']:
                    if db['name'] == database_name:
                        return db['id']
                print(f"Database '{database_name}' not found.")
            else:
                print("No databases found.")
        else:
            print("Error:", response.status_code)
        return -1

    def get_database_info(self):
        if self.current_database_id is None:
            print("No database selected. Use '.use <database_name>' to select a database.")
            return

        url = f'{BASE_URL}/{self.current_database_id}'
        headers = {
            'accept': 'application/json',
            'Authorization': f'Token {self.token}'
        }
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            json_data = response.json()['data']
            table_data = [
                ["Database ID", json_data['id']],
                ["Database Name", json_data['name']],
                ["Client ID", json_data['client_id']],
                ["Status", json_data['status']],
                ["Created At", json_data['created_at']],
                ["Updated At", json_data['updated_at']]
            ]
            database_info = tabulate(table_data, headers=["Attribute", "Value"], tablefmt="fancy_grid")
            print(database_info)
        else:
            print("Error:", response.status_code)

    def do_create(self, arg):
        """Create a new database."""
        if not arg:
            print("Usage: .create <database_name>")
            return

        database_name = arg[0]
        url = f'{BASE_URL}'
        headers = {
            'accept': 'application/json',
            'Authorization': f'Token {self.token}',
            'Content-Type': 'application/json'
        }
        data = {"name": database_name}

        response = requests.post(url, json=data, headers=headers)

        if response.status_code == 202:
            json_data = response.json()
            if 'data' in json_data and 'id' in json_data['data'] and 'name' in json_data['data']:
                db_id = json_data['data']['id']
                db_name = json_data['data']['name']
                print(f"New database created. ID: {db_id}, Name: {db_name}")
                return
            else:
                print("Error: Unexpected response format.")
        else:
            error_detail = response.json().get('detail')
            error_name = response.json().get('name')
            if error_detail:
                print(f"Error: {error_detail}")
            elif error_name:
                print(f"Error: {error_name[0]}")
            else:
                print(f"Error: {response.status_code}")

    def do_destroy(self, arg):
        """ Destroy a database by name. """
        if not arg:
            print("Usage: .destroy <database_name>")
            return

        database_id = self.get_database_id(arg[0])
        if database_id == -1:
            return

        url = f'{BASE_URL}/{database_id}'
        headers = {
            'accept': 'application/json',
            'Authorization': f'Token {self.token}'
        }
        response = requests.delete(url, headers=headers)

        if response.status_code == 202:
            if self.current_database_name == arg[0]:  
                self.current_database_id = None  
                self.current_database_name = None  
            print('Database deleted successfully.')
        else:
            print(f"Error: {response.status_code}")


    def read_sql_from_file(self, file_name):
        """Read SQL statements from a file and execute them."""
        if not os.path.isfile(file_name):
            print(f"File '{file_name}' not found.")
            return

        try:
            with open(file_name, 'r') as file:
                sql_statements = file.read()
        except Exception as e:
            print(f"Error reading file '{file_name}': {e}")
            return

        self.execute_sql_command(sql_statements)

def signal_handler(sig, frame):
    print('\nCtrl+C pressed. Exiting Azion DB Shell.')
    exit()

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    token = os.environ.get('AZION_TOKEN')
    if token is None:
        print("Authorization token not found in environment variable AZION_TOKEN")
        exit(1)
    azion_db_shell = AzionDBShell(token)
    azion_db_shell.cmdloop("Welcome to EdgeSQL Shell. Type '.exit' to quit.")
