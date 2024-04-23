import os
import cmd
import utils
import utils_sql as sql
import edgesql
import edgesql_kaggle as ek
from tabulate import tabulate
import signal
from pathlib import Path
from pathvalidate import ValidationError, validate_filepath
import pandas as pd
from io import StringIO
from tqdm import tqdm

DUMP_SCHEMA_ONLY = 0x1
DUMP_DATA_ONLY = 0x1 << 1
DUMP_ALL = DUMP_SCHEMA_ONLY | DUMP_DATA_ONLY
DUMP_NONE = 0x0

IGNORE_TOKENS = ['--']


class EdgeSQLShell(cmd.Cmd):
    def __init__(self, edgesql):
        """
        Initialize EdgeSQLShell object.

        Args:
            edgesql (EdgeSQL): An instance of the EdgeSQL class.
        """
        super().__init__()
        self.edgeSql = edgesql
        self.prompt = 'EdgeSQL'
        self.update_prompt()
        self.last_command = ''
        self.multiline_command = []
        self.output = ''
        self.outFormat = 'tabular'
        self.transaction = False

    def __command_map(self):
        """Return a dictionary mapping shell commands to corresponding methods."""
        command_mapping = {
            ".exit": self.do_exit,
            ".tables": self.do_tables,
            ".schema": self.do_schema,
            ".databases": self.do_databases,
            ".use": self.do_use,
            ".dbinfo": self.do_dbinfo,
            ".create": self.do_create,
            ".destroy": self.do_destroy,
            ".read": self.do_read,
            ".dump": self.do_dump,
            ".output": self.do_output,
            ".mode": self.do_mode,
            ".import": self.do_import,
            ".import-kaggle": self.do_import_kaggle,
        }
        return command_mapping

    def update_prompt(self):
        """Update the command prompt."""
        if self.edgeSql.get_current_database_name():
            self.prompt = f'EdgeSQL ({self.edgeSql.get_current_database_name()})> '
        else:
            self.prompt = 'EdgeSQL> '

    def do_exit(self, arg):
        """Exit the shell."""
        utils.write_output("Exiting EdgeSQL Shell.")
        return True
    
    def emptyline(self):
        """Ignore empty lines."""
        pass

    def do_create(self, arg):
        """
        Create a new database.

        Args:
            arg (str): The name of the new database.
        """
        if not arg:
            utils.write_output("Usage: .create <database_name>")
            return
        
        args = arg.split()
        if len(args) > 1:
            utils.write_output("Usage: .create <database_name>")
            return

        database_name = arg.strip()
        if not database_name:
            utils.write_output("Error: Database name cannot be empty.")
            return
        
        try:
            self.edgeSql.create_database(database_name)
        except Exception as e:
            utils.write_output(f"Error creating database: {e}")


    def do_destroy(self, arg):
        """
        Destroy a database by name.

        Args:
            arg (str): The name of the database to destroy.
        """
        if not arg:
            utils.write_output("Usage: .destroy <database_name>")
            return
        
        args = arg.split()
        if len(args) > 1:
            utils.write_output("Usage: .destroy <database_name>")
            return
        
        database_name = arg.strip()
        if not database_name:
            utils.write_output("Error: Database name cannot be empty.")
            return

        try:
            database_id = self.edgeSql.get_database_id(database_name)
            if not database_id:
                utils.write_output(f"Database '{database_name}' not found.")
                return
            
            self.edgeSql.destroy_database(database_id)
        except Exception as e:
            utils.write_output(f"Error destroying database: {e}")


    def do_tables(self, arg):
        """List all tables."""
        output = self.edgeSql.list_tables()
        if output is not None:
            rows = output.get('rows')
            columns = output.get('columns')
            if rows and columns:
                self.query_output(rows, columns)
            else:
                utils.write_output("No tables available.")
        else:
            utils.write_output("Error listing tables.")


    def do_schema(self, arg):
        """
        Describe table.

        Args:
            arg (str): The name of the table to describe.
        """
        if not arg:
            utils.write_output("Usage: .schema <table_name>")
            return
        else: 
            table_name = arg.strip()
        
        output = self.edgeSql.describe_table(table_name)
        if output is not None:
            rows = output.get('rows')
            columns = output.get('columns')
            if rows and columns:
                self.query_output(rows, columns)
            else:
                utils.write_output(f"No schema information found for table '{table_name}'.")
        else:
            utils.write_output("Error describing table schema.")


    def do_databases(self, arg):
        """List all databases."""
        db_list = self.edgeSql.list_databases()
        if db_list:
            databases = db_list.get('databases')
            columns = db_list.get('columns')
            if databases and columns:
                formatted_table = tabulate(databases, headers=columns, tablefmt="fancy_grid")
                utils.write_output(formatted_table, self.output)
            else:
                utils.write_output("Error: Invalid database information.")
        else:
            utils.write_output("No databases found.")


    def do_use(self, arg):
        """
        Switch to a database by name.

        Args:
            arg (str): The name of the database to switch to.
        """
        if not arg:
            utils.write_output("Usage: .use <database_name>")
            return
        
        database_name = arg.strip()
        if not database_name:
            utils.write_output("Invalid database name.")
            return

        if self.edgeSql.set_current_database(database_name):
            self.update_prompt()
            utils.write_output(f"Switched to database '{arg}'.")


    def do_dbinfo(self, arg):
        """Get information about the current database."""
        if not self.edgeSql.get_current_database_id():
            utils.write_output("No database selected. Use '.use <database_name>' to select a database.")
            return

        db_info = self.edgeSql.get_database_info()
        if db_info:
            data = db_info.get('table_data')
            columns = db_info.get('columns')
            database_info = tabulate(data, headers=columns, tablefmt="fancy_grid")
            utils.write_output(database_info, self.output)
        else:
            utils.write_output("Error: Unable to fetch database information.")


    def do_output(self, arg):
        """
        Set the output to stdout or file.

        Args:
            arg (str): 'stdout' to output to console, or a file path.
        """
        if not arg:
            utils.write_output("Usage: .output stdout|file_path")
            return
        
        output_mode = arg.split()[0].lower()
        if output_mode == 'stdout':
            self.output = ''
            utils.write_output("Output set to stdout.")
        else:
            file_path = Path(output_mode)
            try:
                validate_filepath(file_path, platform='auto')
                self.output = output_mode
                utils.write_output(f"Output set to file: {output_mode}")
            except ValidationError as e:
                utils.write_output(f"Error: {e}")
                return
    

    def do_mode(self, arg):
        """
        Set output mode.

        Args:
            arg (str): Output mode ('excel', 'tabular', 'csv', 'html', 'markdown', 'raw').
        """
        mode_lst = ['excel', 'tabular','csv','html','markdown','raw']
        arg_lower = arg.lower() if arg else None
    
        if not arg_lower or arg_lower not in mode_lst:
            utils.write_output("Usage: .mode excel|tabular|csv|html|markdown|raw")
            return

        self.outFormat = arg_lower
        utils.write_output(f"Output mode set to: {arg_lower}")


    def do_import(self, arg):
        """
        Import data from FILE into TABLE.

        Args:
            arg (str): File path and table name separated by space.
        """
        if not self.edgeSql.get_current_database_id():
            utils.write_output("No database selected. Use '.use <database_name>' to select a database.")
            return
        
        if not arg:
            utils.write_output("Usage: .import <file> <table>")
            return
        
        args = arg.split()
        if len(args) != 2:
            utils.write_output("Usage: .import <file> <table>")
            return

        file_path, table_name = args

        if self.outFormat not in ['csv', 'excel']:
            utils.write_output('Current mode isn\'t compatible with that operation')
            return
        
        try:
            self.import_data(file_path, table_name)
            utils.write_output(f"Data imported from {file_path} to table {table_name} successfully.")
        except Exception as e:
            utils.write_output(f"Error during import: {e}")

    def do_import_kaggle(self, arg):
        """
        Import data from FILE into TABLE.

        Args:
            arg (str): File path and table name separated by space.
        """
        if not self.edgeSql.get_current_database_id():
            utils.write_output("No database selected. Use '.use <database_name>' to select a database.")
            return
        
        if not arg:
            utils.write_output("Usage: .import-kaggle <dataset> <data_name> <table>")
            return
        
        args = arg.split()
        if len(args) != 3:
            utils.write_output("Usage: .import-kaggle <dataset> <data_name> <table>")
            return

        dataset, data_name, table_name = args
        
        try:
            if self.import_data_kaggle(dataset, data_name, table_name):
                utils.write_output(f"Dataset {dataset} - {data_name} imported from Kaggle to table {table_name} successfully.")
        except Exception as e:
            utils.write_output(f"Error during import: {e}")


    def do_read(self, arg):
        """
        Load SQL statements from file and execute them.

        Args:
            arg (str): File name.
        """
        if not arg:
            utils.write_output("Usage: .read <file_name>")
            return

        file_name = arg
        try:
            if Path(file_name).is_file():
                self.read_sql_from_file(file_name)
                utils.write_output(f"SQL statements from {file_name} executed successfully.")
            else:
                utils.write_output(f"Error: File '{file_name}' not found.")
        except Exception as e:
            utils.write_output(f"Error during execution: {e}")

    def execute_sql_command_multiline(self):
        """Execute a multiline command command."""
        if not self.multiline_command:
            return

        sql_command = ' '.join(self.multiline_command)
        if not sql_command.endswith(';'):
            utils.write_output("Error: Incomplete SQL command. End the command with ';'.")
            return

        try:
            output = self.edgeSql.execute(sql_command)
            if output:
                self.query_output(output['rows'], output['columns'])
        except Exception as e:
             utils.write_output(f"Error executing SQL command: {e}")

        self.multiline_command = []  # Reset multiline command buffer


    def do_dump(self, arg):
        """
        Render database structure as SQL.

        Args:
            arg (str): Optional arguments '--schema-only', '--data-only', or table name(s).
        """
        dump_type = DUMP_NONE

        if not arg:
            self.dump()
        else:
            args = arg.split()

            if '--schema-only' in args:
                args.remove('--schema-only')
                dump_type = dump_type | DUMP_SCHEMA_ONLY
            if '--data-only' in args:
                args.remove('--data-only')
                dump_type = dump_type | DUMP_DATA_ONLY

            if dump_type == DUMP_NONE:
                self.dump(arg=args, dump=DUMP_ALL)
            else:
                self.dump(arg=args,dump=dump_type)


    def dump_table(self, table_name, dump=DUMP_ALL, batch_size=1000):
        """
        Dump table structure and data as SQL.

        Args:
            table_name (str): Name of the table to dump.
            dump (int, optional): Flag indicating what to dump (schema only, data only, or both). Defaults to DUMP_ALL.
            batch_size (int, optional): Size of each data batch for fetching. Defaults to 1000.
        """
        try:
            # Check if the table exists
            if not self.edgeSql.exist_table(table_name):
                utils.write_output(f"Table '{table_name}' not found", self.output)
                return

            # Dump schema if requested
            if dump & DUMP_SCHEMA_ONLY:
                # Table structure
                table_output = self.edgeSql.execute(f"SELECT sql FROM sqlite_schema WHERE type like 'table' \
                                        AND sql NOT NULL \
                                        AND name like '{table_name}' \
                                        ORDER BY tbl_name='sqlite_sequence', rowid;")
                if table_output:
                    statement = table_output['rows'][0][0]
                    formatted_query = sql.format_sql(f'CREATE TABLE IF NOT EXISTS {statement[13:]};')
                    utils.write_output(formatted_query, self.output)
                
                # Indexes, Triggers, and Views
                additional_objects_output = self.edgeSql.execute(f"SELECT sql FROM sqlite_schema \
                                        WHERE sql NOT NULL \
                                        AND tbl_name like '{table_name}' \
                                        AND type IN ('index','trigger','view');")
                if additional_objects_output:
                    additional_objects = additional_objects_output['rows']
                    for obj in additional_objects:
                        statement = obj[0]
                        if 'INDEX' in statement.upper():
                            formatted_query = sql.format_sql(f'CREATE INDEX IF NOT EXISTS {statement[13:]};')
                        elif 'TRIGGER' in statement.upper():
                            formatted_query = sql.format_sql(f'CREATE TRIGGER IF NOT EXISTS {statement[15:]};')
                        elif 'VIEW' in statement.upper():
                            formatted_query = sql.format_sql(f'CREATE VIEW IF NOT EXISTS {statement[12:]};')
                        else:
                            formatted_query = ''
                        
                        if formatted_query:
                            utils.write_output(formatted_query, self.output)

            # Dump data if requested
            if dump & DUMP_DATA_ONLY:
                # Get total count of rows in the table
                count_output = self.edgeSql.execute(f'SELECT COUNT(*) FROM {table_name};')
                if count_output and count_output['rows']:
                    total_rows = count_output['rows'][0][0]

                    # Fetch data in batches and generate SQL insert statements
                    offset = 0
                    while offset < total_rows:
                        limit = min(batch_size, total_rows - offset)  # Calculate limit for this batch
                        data_output = self.edgeSql.execute(f'SELECT * FROM {table_name} LIMIT {limit} OFFSET {offset};')
                        if data_output:
                            df = pd.DataFrame(data_output['rows'], columns=data_output['columns'])
                            sql_commands = sql.generate_insert_sql(df, table_name)
                            for cmd in sql_commands:
                                utils.write_output(cmd, self.output)
                        
                        offset += batch_size

        except Exception as e:
            utils.write_output(f"Error dumping table '{table_name}': {e}")

    
    def dump(self, arg=False, dump=DUMP_ALL):
        """
        Dump database structure and data as SQL.

        Args:
            arg (list, optional): List of specific tables to dump. Defaults to False.
            dump (int, optional): Flag indicating what to dump (schema only, data only, or both). Defaults to DUMP_ALL.
        """
        try:
            utils.write_output("PRAGMA foreign_keys=OFF;", self.output)
            utils.write_output("BEGIN TRANSACTION;", self.output)

            # Dump all tables if no specific tables are provided
            if not arg or len(arg) == 0:
                tables_output = self.edgeSql.execute("SELECT name FROM sqlite_schema WHERE type like 'table';")
                if tables_output:
                    table_lst = tables_output['rows']
                    for table in table_lst:
                        table_name = table[0]

                        if table_name == "sqlite_sequence":
                            utils.write_output("DELETE FROM sqlite_sequence;", self.output);
                        elif table_name == "sqlite_stat1":
                            utils.write_output("ANALYZE sqlite_master;", self.output);
                        elif table_name.startswith("sqlite_"):
                            continue
                        else:
                            self.dump_table(table_name, dump)
            else: # Dump particular table(s)
                for tbl in arg:
                    self.dump_table(tbl, dump)

            utils.write_output("COMMIT;", self.output)
            utils.write_output("PRAGMA foreign_keys=ON;", self.output)

        except Exception as e:
            utils.write_output(f"Error dumping database: {e}")

    def default(self, arg):
        """Execute SQL command and handle multiline input."""
        if arg.strip():
            # New command
            self.last_command = arg
        
            # Check if the command starts with a dot (indicating a shell command)
            if arg.startswith("."):
                command, *args = arg.split(" ")
                command_map = self.__command_map()  # Get the command mapping
                # Execute the corresponding method if the command is recognized
                if command in command_map:
                    return command_map[command](" ".join(args))
                else:
                    utils.write_output("Invalid command.")
            elif self.multiline_command:
                if utils.contains_any(arg,IGNORE_TOKENS):
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
                if utils.contains_any(arg, IGNORE_TOKENS + ['END;']):
                    pass
                elif 'transaction' in arg.lower():
                    self.transaction = True
                    pass
                elif arg.endswith(';') and not self.transaction:
                    # Single-line command, execute immediately
                    output = self.edgeSql.execute(arg)
                    if output:
                        self.query_output(output['rows'], output['columns'])
                    return
                else:
                    # Multi-line command, accumulate lines
                    self.multiline_command.append(arg)
        else:
            pass


    def query_output(self, rows, columns):
        """Format and output query results."""
        df = pd.DataFrame(rows,columns=columns)

        if self.outFormat == 'tabular':
            formatted_data = tabulate(df.to_dict(orient='records'), headers="keys", tablefmt='fancy_grid')
            utils.write_output(formatted_data, self.output)
        elif self.outFormat == 'markdown':
            formatted_data = tabulate(df.to_dict(orient='records'), headers="keys", tablefmt='pipe')
            utils.write_output(formatted_data, self.output)
        elif self.outFormat == 'csv':
            if self.output == '':
                buffer = StringIO()
                df.to_csv(buffer, index=False)
                buffer.seek(0)
                utils.write_output(buffer.getvalue(), self.output)
            else:
                df.to_csv(self.output, index=False)
        elif self.outFormat == 'excel':
            if self.output == '':
                buffer = StringIO()
                df.to_csv(buffer, index=False)
                buffer.seek(0)
                utils.write_output(buffer.getvalue(), self.output)
            elif not self.output.endswith(".xlsx"): # engine=io.excel.xlsx.writer
                utils.write_output('For "excel" mode, the output file must have the extension .xlsx', '')
            else:
                df.to_excel(self.output, index=False)
        elif self.outFormat == 'html':
            if self.output == '':
                buffer = StringIO()
                df.to_html(buffer, index=False)
                buffer.seek(0)
                utils.write_output(buffer.getvalue(), self.output)
            else:
                df.to_html(self.output, index=False)
        else: # raw
            if self.output == '':
                buffer = StringIO()
                df.to_csv(buffer, sep=' ', index=False)
                buffer.seek(0)
                utils.write_output(buffer.getvalue(), self.output)
            else:
                df.to_csv(self.output, sep=' ', index=False)


    def read_sql_from_file(self, file_name):
        """Read SQL statements from a file and execute them."""
        if not os.path.isfile(file_name):
            utils.write_output(f"File '{file_name}' not found.")
            return

        try:
            with open(file_name, 'r') as file:
                sql_statements = file.read()
        except Exception as e:
            utils.write_output(f"Error reading file '{file_name}': {e}")
            return

        try:
            self.edgeSql.execute(sql_statements)
        except Exception as e:
            utils.write_output(f'Error executing SQL statements from file: {e}')


    def import_data(self, file, table_name, chunk_size=1000):
        """
        Import data from a file into a database table.

        Args:
            file (str): The path to the file containing the data to be imported.
            table_name (str): The name of the database table where the data will be imported.

        Returns:
            None

        Raises:
            FileNotFoundError: If the specified file does not exist.
            pd.errors.EmptyDataError: If the specified file is empty or contains no data.
            pd.errors.ParserError: If an error occurs while parsing the file (e.g., incorrect format).

        Note:
            This function supports importing data from CSV and Excel files.
        """
        try:
            # Validate file existence
            if not os.path.isfile(file):
                utils.write_output(f'Error: The specified file "{file}" does not exist.')
                return
            
            # Validate table name
            if not table_name:
                utils.write_output("Error: Table name cannot be empty.")
                return
            
            # Read data from file
            if self.outFormat == 'csv':
                df = pd.read_csv(file)
            elif self.outFormat == 'excel':
                df = pd.read_excel(file)
            else:
                raise ValueError("Unsupported output format. Supported formats are CSV and Excel.")
    
            self._import_data(df, table_name, chunk_size)
        except FileNotFoundError as e:
            utils.write_output(str(e))
        except pd.errors.EmptyDataError:
            utils.write_output(f'The specified file "{file}" is empty or contains no data.')
        except pd.errors.ParserError:
            utils.write_output(f'An error occurred while parsing "{file}". Please check if the file format is correct.')
        except Exception as e:
            utils.write_output(f'Error importing data: {e}')

    def _import_data(self, dataset, table_name, chunk_size=1000):
        """
        Import data into a specified database table in chunks with a progress bar.

        Args:
            dataset (pandas.DataFrame): The dataset to be imported.
            table_name (str): The name of the database table where the data will be imported.
            chunk_size (int): Size of each data chunk for insertion. Default is 1000.

        Returns:
            bool: True if the import is successful, False otherwise.
        """
        try:
            # Check if the table exists and create it if necessary
            if not self.edgeSql.exist_table(table_name):
                create_sql = sql.generate_create_table_sql(dataset, table_name)
                self.edgeSql.execute(create_sql)

            total_chunks = len(dataset) // chunk_size + (1 if len(dataset) % chunk_size != 0 else 0)

            with tqdm(total=total_chunks, desc="Progress", unit="chunk") as progress_bar:
                for i, chunk in enumerate([dataset[i:i + chunk_size] for i in range(0, len(dataset), chunk_size)], 1):
                    # Generate SQL for data insertion
                    insert_sql = sql.generate_insert_sql(chunk, table_name)
                    self.edgeSql.execute(insert_sql)

                    # Update progress bar
                    progress_bar.update(1)

            return True  # Import successful
        except Exception as e:
            utils.write_output(f'Error inserting data into database: {e}')
            return False

    def import_data_kaggle(self, dataset_name, data_file, table_name, chunk_size=1000):
        """
        Import data from a Kaggle dataset into a specified database table.

        Args:
            dataset_name (str): The name of the dataset on Kaggle to be imported.
            table_name (str): The name of the database table where the data will be imported.

        Returns:
            bool: True if the import is successful, False otherwise.
        """
        if not table_name:
            utils.write_output("Error: Please provide a valid table name.")
            return False
        
        if not data_file:
            utils.write_output("Error: Please provide a valid data file from dataset.")
            return False
        
        if not dataset_name:
            utils.write_output("Error: Please provide a valid dataset name.")
            return False
        
        username = os.environ.get('KAGGLE_USERNAME')
        if username is None:
            utils.write_output("Kaggle username account not found in environment variable KAGGLE_USERNAME")
            return False
        
        api_key = os.environ.get('KAGGLE_KEY')
        if api_key is None:
            utils.write_output("Kaggle API Key not found in environment variable KAGGLE_KEY")
            return False
        
        try:
            # Initialize Kaggle API
            kaggle = ek.EdgSQLKaggle(username, api_key)
            
            # Import dataset from Kaggle
            import_success = self._import_kaggle_dataset(kaggle, dataset_name, data_file, table_name, chunk_size)
            if not import_success:
                utils.write_output(f"Error: Failed to import dataset '{dataset_name}' from Kaggle.")
                return False
            
            return True  # Successful import
        except Exception as e:
            utils.write_output(f'Error importing Kaggle data: {e}')
            return False  # Import failed


    def _import_kaggle_dataset(self, kaggle, dataset_name, data_file, table_name, chunk_size):
        """
        Import dataset from Kaggle into a specified database table in chunks.

        Args:
            kaggle (kg.EdgSQLKaggle): Initialized Kaggle API object.
            dataset_name (str): The name of the dataset on Kaggle to be imported.
            table_name (str): The name of the database table where the data will be imported.
            chunk_size (int): Size of each data chunk for insertion.

        Returns:
            bool: True if the import is successful, False otherwise.
        """
        # Import dataset from Kaggle
        import_success = kaggle.import_dataset(dataset_name, data_file)
        if not import_success:
            return False
        
        dataset = kaggle.get_dataset()
        if dataset is None:
            utils.write_output("Error: No dataset found.")
            return False

        return self._import_data(dataset, table_name, chunk_size)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, utils.signal_handler)
    token = os.environ.get('AZION_TOKEN')
    if token is None:
        utils.write_output("Authorization token not found in environment variable AZION_TOKEN")
        exit(1)

    base_url = os.environ.get('AZION_BASE_URL')
    edgSql = edgesql.EdgeSQL(token, base_url)
    azion_db_shell = EdgeSQLShell(edgSql)
    azion_db_shell.cmdloop("Welcome to EdgeSQL Shell. Type '.exit' to quit.")
