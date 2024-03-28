import os
import requests
import cmd
from tabulate import tabulate
import signal
import sqlparse
from pathlib import Path
from pathvalidate import ValidationError, validate_filepath
import pandas as pd
from io import StringIO

BASE_URL = 'https://api.azion.com/v4/edge_sql/schemas'
IGNORE_TOKENS = ['--']

class EdgeSQLShell(cmd.Cmd):
    def __init__(self, token):
        super().__init__()
        self.token = token
        self.current_database_id = None
        self.current_database_name = None
        self.prompt = 'EdgeSQL'
        self.update_prompt()
        self.last_command = ''
        self.multiline_command = []
        self.buffer = ''
        self.output = ''
        self.outFormat = 'tabular'
        self.transaction = False

    def update_prompt(self):
        if self.current_database_name:
            self.prompt = f'EdgeSQL ({self.current_database_name})> '
        else:
            self.prompt = 'EdgeSQL> '

    def do_exit(self, arg):
        """Exit the shell."""
        write_output("Exiting EdgeSQL Shell.")
        return True
    
    def emptyline(self):
        """Ignore empty lines."""
        pass

    def do_tables(self, arg):
        """List all tables."""
        self.execute_sql_command("PRAGMA table_list;")

    def do_schema(self, arg):
        """Describe table."""
        if not arg:
            write_output("Usage: .schema <table_name>")
            return
        self.execute_sql_command(f"PRAGMA table_info({arg});")

    def do_databases(self, arg):
        """List all databases."""
        self.execute_database_command()

    def do_use(self, arg):
        """Switch to a database by name."""
        if not arg:
            write_output("Usage: .use <database_name>")
            return
        self.set_current_database(arg)

    def do_dbinfo(self, arg):
        """Get information about the current database."""
        self.get_database_info()

    def do_output(self, arg):
        """Set the output to stdout or file."""
        if not arg:
            write_output("Usage: .output stdout|file_path")
            return
        
        args = arg.split()
        if args[0] == 'stdout':
            self.output = ''
        else:
            file_path = Path(args[0])
            try:
                validate_filepath(args[0], platform='auto')
            except ValidationError as e:
                write_output(f"Error: {e}")
                return

            self.output = args[0]

    def do_mode(self, arg):
        """Set output mode."""
        if not arg:
            write_output("Usage: .mode tabular|csv|html|markdown|raw")
            return
        self.outFormat = arg

    def do_import(self, arg):
        """Import data from FILE into TABLE."""
        if not arg:
            write_output("Usage: .import file table")
            return
        
        args = arg.split()
        if len(args) != 2:
            write_output("Usage: .import file table")
            return

        if (self.outFormat != 'csv'):
            write_output('Current mode ins\'t compatible with that operation')
            return
        
        file = args[0]
        table_name = args[1]

        self.import_data(file, table_name)


    def do_read(self, arg):
        """Load SQL statements from file and execute them."""
        if not arg:
            write_output("Usage: .read <file_name>")
            return
        self.read_sql_from_file(arg)

    def execute_sql_command_multiline(self):
        """Execute a multiline command command"""
        sql_command = ' '.join(self.multiline_command)
        try:
            self.execute_sql_command(sql_command)
        except Exception as e:
             write_output(f"Error executing SQL command: {e}")
        self.multiline_command = []  # Reset multiline command buffer

    def do_dump(self, arg):
        """Render database structure as SQL."""
        if not arg:
            write_output("Usage: .dump <table_name>")
            return
        else:
            self.dump_table(arg)
    
    def dump_table(self, table_name):
        self.buffer = "PRAGMA foreign_keys=OFF;"
        self.buffer = ''.join([self.buffer, "BEGIN TRANSACTION;"])
        self.execute_sql_command(f"select sql from sqlite_schema where tbl_name = '{table_name}';", outInternal=True)
        self.buffer = ''.join([self.buffer, "COMMIT;"])
        self.buffer = ''.join([self.buffer, "PRAGMA foreign_keys=ON;"])
        self.buffer = ''.join([self.buffer, ""])
        formatted_query = sqlparse.format(self.buffer, reindent=True, keyword_case='upper')
        write_output(formatted_query, self.output)
        self.buffer = ''

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
                elif command == ".dump":
                    return self.do_dump(" ".join(args))
                elif command == ".output":
                    return self.do_output(" ".join(args))
                elif command == ".mode":
                    return self.do_mode(" ".join(args))
                elif command == ".import":
                    return self.do_import(" ".join(args))
                else:
                    write_output("Invalid command.")
            elif self.multiline_command:
                if contains_any(arg,IGNORE_TOKENS):
                    pass
                elif any('begin' in cmd.lower() for cmd in self.multiline_command):
                    # Multi-line command with 'begin', accumulate lines
                    if 'end;' in arg.lower():
                        self.multiline_command.append(arg)
                        self.execute_sql_command_multiline()
                        return
                    else:
                        self.multiline_command.append(arg)
                else:
                    # Multi-line single command
                    if ';' in arg.lower() and not self.transaction:
                        self.multiline_command.append(arg)
                        self.execute_sql_command_multiline()
                        return
                    elif 'commit' in arg.lower():
                        self.execute_sql_command_multiline()
                        return
                    elif 'rollback' in arg.lower():
                        self.transaction = False
                        self.multiline_command = ''
                        return
                    else:
                        self.multiline_command.append(arg)
                    
            else:
                if contains_any(arg, IGNORE_TOKENS + ['END;']):
                    pass
                elif 'transaction' in arg.lower():
                    self.transaction = True
                    pass
                elif arg.endswith(';') and not self.transaction:
                    # Single-line command, execute immediately
                    self.execute_sql_command(arg)  
                    return
                else:
                    # Multi-line command, accumulate lines
                    self.multiline_command.append(arg)
        else:
            pass

    def query_output(self, rows, columns):
        df = pd.DataFrame(rows,columns=columns)

        if self.outFormat == 'tabular':
            formatted_data = tabulate(df.to_dict(orient='records'), headers="keys", tablefmt='fancy_grid')
            write_output(formatted_data, self.output)
        elif self.outFormat == 'markdown':
            formatted_data = tabulate(df.to_dict(orient='records'), headers="keys", tablefmt='pipe')
            write_output(formatted_data, self.output)
        elif self.outFormat == 'csv':
            if self.output == '':
                buffer = StringIO()
                df.to_csv(buffer, index=False)
                buffer.seek(0)
                write_output(buffer.getvalue(), self.output)
            else:
                df.to_csv(self.output, index=False)
        elif self.outFormat == 'html':
            if self.output == '':
                buffer = StringIO()
                df.to_html(buffer, index=False)
                buffer.seek(0)
                write_output(buffer.getvalue(), self.output)
            else:
                df.to_html(self.output, index=False)
        else: # raw
            if self.output == '':
                buffer = StringIO()
                df.to_csv(buffer, sep=' ', index=False)
                buffer.seek(0)
                write_output(buffer.getvalue(), self.output)
            else:
                df.to_csv(self.output, sep=' ', index=False)
            

    def execute_sql_command(self, buffer, outInternal=False):
        if self.current_database_id is None:
            write_output("No database selected. Use '.use <database_name>' to select a database.")
            return

        url = f'{BASE_URL}/{self.current_database_id}/execute'
        headers = {
            'accept': 'application/json',
            'Authorization': f'Token {self.token}',
            'Content-Type': 'application/json'
        }

        if type(buffer) == list:
            sql_commands = buffer
        else:
            sql_commands = self.split_sql_buffer(buffer)

        data = {"statements": sql_commands}
        response = requests.post(url, json=data, headers=headers)
        self.transaction = False

        if response.status_code == 200:
            json_data = response.json()
            if 'data' in json_data and isinstance(json_data['data'], list) and json_data['data']:
                result_data = json_data['data'][0]
                if 'error' in result_data:
                    write_output(f'"Error:", {result_data["error"]}')
                else:
                    results = result_data.get('results', {})
                    columns = results.get('columns', [])
                    rows = results.get('rows', [])
                    if columns and rows:
                        if outInternal:
                            for row in rows:
                                result = ' '.join(map(str, row))
                                self.buffer = ''.join([self.buffer, result])
                        else:
                            self.query_output(rows, columns)
            else:
                write_output("Error: Empty or invalid response data.")
        else:
            write_output(f'"Error:", {response.status_code}')

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
            write_output(formatted_table, self.output)
        else:
            write_output(f'"Error:", {response.status_code}')

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
                    write_output(f"Switched to database '{database_name}'.")
                    return
            write_output(f"Database '{database_name}' not found.")
        else:
            write_output(f'"Error:", {response.status_code}')

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
                write_output(f"Database '{database_name}' not found.")
            else:
                write_output("No databases found.")
        else:
            write_output(f'"Error:", {response.status_code}')
        return -1

    def get_database_info(self):
        if self.current_database_id is None:
            write_output("No database selected. Use '.use <database_name>' to select a database.")
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
            write_output(database_info, self.output)
        else:
            write_output(f'"Error:", {response.status_code}')

    def do_create(self, arg):
        """Create a new database."""
        if not arg:
            write_output("Usage: .create <database_name>")
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
                write_output(f"New database created. ID: {db_id}, Name: {db_name}")
                return
            else:
                write_output("Error: Unexpected response format.")
        else:
            error_detail = response.json().get('detail')
            error_name = response.json().get('name')
            if error_detail:
                write_output(f"Error: {error_detail}")
            elif error_name:
                write_output(f"Error: {error_name[0]}")
            else:
                write_output(f"Error: {response.status_code}")

    def do_destroy(self, arg):
        """ Destroy a database by name. """
        if not arg:
            write_output("Usage: .destroy <database_name>")
            return

        database_id = self.get_database_id(arg)
        if database_id == -1:
            return

        url = f'{BASE_URL}/{database_id}'
        headers = {
            'accept': 'application/json',
            'Authorization': f'Token {self.token}'
        }
        response = requests.delete(url, headers=headers)

        if response.status_code == 202:
            if self.current_database_name == arg:  
                self.current_database_id = None  
                self.current_database_name = None  
            write_output('Database deleted successfully.')
        else:
            write_output(f"Error: {response.status_code}")


    def read_sql_from_file(self, file_name):
        """Read SQL statements from a file and execute them."""
        if not os.path.isfile(file_name):
            write_output(f"File '{file_name}' not found.")
            return

        try:
            with open(file_name, 'r') as file:
                sql_statements = file.read()
        except Exception as e:
            write_output(f"Error reading file '{file_name}': {e}")
            return

        self.execute_sql_command(sql_statements)

    def generate_create_table_sql(self, df, table_name):
        columns = []
        for column_name, dtype in df.dtypes.items():
            if dtype == 'object':
                columns.append(f"{column_name} VARCHAR")
            elif dtype == 'int64':
                columns.append(f"{column_name} INT")
            elif dtype == 'float64':
                columns.append(f"{column_name} FLOAT")
            # Add more conditions as needed for other data types
        
        sql = f"CREATE TABLE {table_name} (\n"
        sql += ",\n".join(columns)
        sql += "\n);"
        return sql
    
    def generate_insert_sql(self, df, table_name):
        sql_commands = []
        for index, row in df.iterrows():
            values = ", ".join([f"'{value}'" if isinstance(value, str) else str(value) for value in row])
            sql = f"INSERT INTO {table_name} VALUES ({values});"
            sql_commands.append(sql)
        return sql_commands
    
    def table_exits(self, table_name):
        self.buffer = ''
        self.execute_sql_command(f"select sql from sqlite_schema where tbl_name = '{table_name}';", outInternal=True)
        if self.buffer != '':
            return True
        
        return False

    def import_data(self, file, table_name):
        try:
            df = pd.read_csv(file)
        except FileNotFoundError:
            write_output(f'Error: The specified file "{file}" does not exist.')
        except pd.errors.EmptyDataError:
            write_output(f'Error: The specified file "{file}" is empty or contains no data.')
        except pd.errors.ParserError:
            write_output(f'Error: An error occurred while parsing "{file}". Please check if the file format is correct.')
        else:
            if self.table_exits(table_name) == False:
                # If the table does not exist, so create one
                sql = self.generate_create_table_sql(df, table_name)
                self.execute_sql_command(sql)
            
            sql = self.generate_insert_sql(df, table_name)
            self.execute_sql_command(sql)


def signal_handler(sig, frame):
    write_output('\nCtrl+C pressed. Exiting EdgeSQL Shell.')
    exit()

def contains_any(arg, substrings):
    """
    Checks if the string contains any of the substrings provided.

    Args:
        arg (str): The string to be checked.
        substrings (list): A list of substrings to be checked.

    Returns:
        bool: True if the string contains any of the substrings, False otherwise.
    """
    for substring in substrings:
        if substring.lower() in arg.lower():
            return True
    return False

def write_output(message, destination=''):
    """
    Writes a message to either stdout or a specified file.

    Args:
        message (str): The message to be written.
        destination (str or file object, optional): The destination for writing. Default is stdout.

    Returns:
        None
    """

    if destination == '':
        print(message)
    else:
        with open(destination, 'w') as file:
            file.write(message+'\n')

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    token = os.environ.get('AZION_TOKEN')
    if token is None:
        write_output("Authorization token not found in environment variable AZION_TOKEN")
        exit(1)
    azion_db_shell = EdgeSQLShell(token)
    azion_db_shell.cmdloop("Welcome to EdgeSQL Shell. Type '.exit' to quit.")
