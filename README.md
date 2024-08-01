# EdgeSQL Shell

EdgeSQL Shell is a command-line interface (CLI) tool for interacting with Azion EdgeSQL Database, allowing users to manage databases, execute SQL commands, and perform various database operations.

## EdgeSQL Shell Documentation Index

1. [Features](#features)
2. [Requirements](#requirements)
3. [Installation](#installation)
4. [Usage](#usage)
5. [Other Settings](#other-settings)
   - [Setting Custom Azion API Entrypoint](#setting-custom-azion-api-entrypoint)
   - [Setting Kaggle Credentials](#setting-kaggle-credentials)
   - [Setting MySQL Credentials](#setting-mysql-credentials)
   - [Setting PostgreSQL Credentials](#setting-postgresql-credentials)
   - [Setting Turso Credentials](#setting-turso-credentials)
7. [Contributing](#contributing)
8. [License](#license)

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
- Output on formats Tabular, CSV, JSON, HTML, Markdown, and Raw
- Data Importation:
	- Adaptive chunk estimation
	- Vector Similarity Search support:
		- Datatype (F32_BLOB / FLOAT32)
		- Use vector function to convert vector from string representation into the binary format
	- From file files: CSV or XLSX capability
	- From databases: Mysql or PostgreSQL
	- From Kaggle Datasets
	- From libSQL from Turso
- Error handling and graceful exit
- Interactive and noninteractive execution

## Requirements

- Python 3.x
- Library Psycopg2
- MySQL Connector/Python 

## Installation

1. Clone this repository:

   ```bash
   git clone git@github.com:aziontech/edgesql-shell.git
   ```
2. Install the system dependencies:
   
	- [mysql-connector-python](https://pypi.org/project/mysql-connector-python/)
	- [psycopg2](https://pypi.org/project/psycopg2/)
   
3. Install the Python dependencies:

   ```bash
   python -m venv env
   source env/bin/activate
   brew install postgresql
   pip install -r requirements.txt
   ```

## Usage

1. Set your Azion authentication token as an environment variable:

   ```bash
    export AZION_TOKEN="your_auth_token_here"
   ```

2. Run the EdgeSQL Shell:

   For interactive mode:
   
   ```bash
   python edgesql-shell.py
   ```

   For noninteractive mode:

   ```bash
    python3 edgesql-shell.py -n -c ".use MyDB2024" -c ".tables"
   ```

4. Use the commands listed below to interact with the EdgeSQL service:

   ```bash
   .tables				                 # List all tables
   .schema <table_name>		                 # Describe table schema
   .dump [--schema-only|--data-only] <table_name>   # Render database structure as SQL
   .databases			                 # List all databases
   .use <database_name>		                 # Switch to a database by name
   .dbinfo				                 # Get information about the current database
   .read <file_name>		                 # Load and execute SQL statements from a file
   .create <database_name>		                 # Create a new database
   .destroy <database_name>	                 #  Destroy a database by name
   .output stdout|file_path                         # Set the output to stdout or file
   .mode tabular|csv|html|markdown|raw              # Set output mode
   .import params table                             # Import data from file|mysql|postgres|kaggle|turso into TABLE
   .dbsize				                 # Get the size of the current database in MB
   .exit				                 # Exit the EdgeSQL Shell
   ```
   
## Other Settings
### Setting Custom Azion API Entrypoint ###
 
 ```bash
   export AZION_BASE_URL="custom.api.azion.com"
 ```
 
### Setting Kaggle Credentials ###
 
 ```bash
   export KAGGLE_USERNAME="username"
   export KAGGLE_KEY="kaggle_api_key"
 ```
 
### Setting MySQL Credentials ###
 
 ```bash
   export MYSQL_USERNAME="username"
   export MYSQL_PASSWORD="password"
   export MYSQL_HOST="host_address"
 ```
 
 Optional settings:
 
 ```bash
   export MYSQL_PORT=<port>
   
   # For TLS connection
   export MYSQL_SSL_CA="ssl_ca"
   export MYSQL_SSL_CERT="ssl_cert"
   export MYSQL_SSL_KEY="ssl_key"
   export MYSQL_SSL_VERIFY_CERT=True|False
 ```
 

### Setting PostgreSQL Credentials ###
 
 ```bash
   export POSTGRES_USERNAME="username"
   export POSTGRES_PASSWORD="password"
   export POSTGRES_HOST="host_address"
 ```

 Optional settings:
 
 ```bash
   export POSTGRES_PORT=<port>
   
   # For TLS connection
   export POSTGRES_SSL_CA="ssl_ca"
   export POSTGRES_SSL_CERT="ssl_cert"
   export POSTGRES_SSL_KEY="ssl_key"
   export POSTGRES_SSL_VERIFY_CERT=True|False
 ```

### Setting Turso Credentials ###

  ```bash
   export TURSO_DATABASE_URL=<https://<db_name>-<organization>.turso.io
   export TURSO_AUTH_TOKEN=<token>
 ```
 
  Optional settings:
 
 ```bash
   export TURSO_ENCRYPTION_KEY=<encryption_key>
  ```	

  Tips for getting database credentials:
  
  Get the database URL:
  
 ```bash
   turso db show --url <database-name>
  ```
  
 Get the database authentication token:
 
  ```bash
   turso db tokens create <database-name>
  ```


## Contributing

Contributions are welcome! If you encounter any issues or have suggestions for improvements, please open an issue or submit a pull request.

## License

This project is licensed under the MIT License.
