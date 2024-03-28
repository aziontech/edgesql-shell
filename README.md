# EdgeSQL Shell

EdgeSQL Shell is a command-line interface (CLI) tool for interacting with Azion EdgeSQL Database, allowing users to manage databases, execute SQL commands, and perform various database operations.

## Features

- List all tables in a database
- Describe table schema
- Dump table structure as SQL
- List all databases
- Switch to a database by name
- Retrieve information about the current database
- Load and execute SQL statements from a file
- Create, destroy, or list databases
- Support for multiline SQL commands
- Transaction support
- Output to standard output or file
- Output on formats Tabular, CSV, HTML, Markdown, and Raw
- Import CSV capability
- Error handling and graceful exit

## Requirements

- Python 3.x
- Requests library
- Tabulate library
- SQLParse library
- PathValidate library
- Pandas library

## Installation

1. Clone this repository:

   ```bash
   git clone git@github.com:aziontech/edgesql-shell.git
   ```
   
2. Install the dependencies:

   ```bash
   pip install -r requirements.txt
   ```

## Usage

1. Set your Azion authentication token as an environment variable:

   ```bash
    export AZION_TOKEN="your_auth_token_here"
   ```

2. Run the EdgeSQL Shell:

   ```bash
   python EdgeSQLShell.py
   ```

3. Use the commands listed below to interact with the EdgeSQL service:

   ```bash
   .tables				   # List all tables
   .schema <table_name>		   # Describe table schema
   .dump <table_name> 		   # Render database structure as SQL
   .databases			   # List all databases
   .use <database_name>		   # Switch to a database by name
   .dbinfo				   # Get information about the current database
   .read <file_name>		   # Load and execute SQL statements from a file
   .create <database_name>		   # Create a new database
   .destroy <database_name>	   # Destroy a database by name
   .output stdout|file_path           # Set the output to stdout or file
   .mode tabular|csv|html|markdown|raw # Set output mode
   .import file table                 # Import data from FILE into TABLE
   .exit				   # Exit the EdgeSQL Shell
   ```

## Contributing

Contributions are welcome! If you encounter any issues or have suggestions for improvements, please open an issue or submit a pull request.

## License

This project is licensed under the MIT License.
