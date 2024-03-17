# EdgeSQL Shell

EdgeSQL Shell is a command-line interface (CLI) tool for interacting with Azion EdgeSQL service, allowing users to manage databases, execute SQL commands, and perform various database operations.

## Features

- List all tables in a database
- Describe table schema
- List all databases
- Switch to a database by name
- Get information about the current database
- Load and execute SQL statements from a file
- Create a new database
- Destroy a database by name
- Support for multiline SQL commands
- Error handling and graceful exit

## Requirements

- Python 3.x
- Requests library
- Tabulate library
- SQLParse library

## Installation

1. Clone this repository:

   ```bash
   git clone https://github.com/your_username/EdgeSQL-Shell.git
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
.tables						# List all tables
.schema <table_name>		# Describe table schema
.databases					# List all databases
.use <database_name>		# Switch to a database by name
.dbinfo						# Get information about the current database
.read <file_name>			# Load and execute SQL statements from a file
.create <database_name>		# Create a new database
.destroy <database_name>	# Destroy a database by name
.exit						# Exit the EdgeSQL Shell
```

## Contributing

Contributions are welcome! If you encounter any issues or have suggestions for improvements, please open an issue or submit a pull request.

## License

This project is licensed under the MIT License.
