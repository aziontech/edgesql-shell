import utils
import re
#command databases
def do_databases(shell, arg):
    """List all databases."""
    try:
        db_list = shell.edgeSql.list_databases()
        if db_list:
            databases = db_list.get('databases')
            columns = db_list.get('columns')
            if databases and columns:
                shell.query_output(databases, columns)
            else:
                utils.write_output("Error: Invalid database information.")
        else:
            utils.write_output("No databases found.")
    except Exception as e:
        raise RuntimeError(f"{e}") from e

#command .use
def do_use(shell, arg):
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

    try:
        if shell.edgeSql.set_current_database(database_name):
            shell.update_prompt()
            utils.write_output(f"Switched to database '{arg}'.")
        else:
            utils.write_output(f"Failed to switch to database '{arg}'.")
    except Exception as e:
        raise RuntimeError(f"{e}") from e

#comand .dbinfo
def do_dbinfo(shell, arg):
    """Get information about the current database."""
    if not shell.edgeSql.get_current_database_id():
        utils.write_output("No database selected. Use '.use <database_name>' to select a database.")
        return

    try:
        db_info = shell.edgeSql.get_database_info()
        if db_info:
            data = db_info.get('table_data')
            columns = db_info.get('columns')
            shell.query_output(data, columns)
        else:
            utils.write_output("Error: Unable to fetch database information.")
    except Exception as e:
        raise RuntimeError(f"{e}") from e

# Command .dbsize
def do_dbsize(shell, arg):
    """Get the size of the current database in MB."""
    if not shell.edgeSql.get_current_database_id():
        utils.write_output("No database selected. Use '.use <database_name>' to select a database.")
        return

    try:
        output = shell.edgeSql.get_database_size()
        if not output['success']:
            utils.write_output(f"{output['error']}")
            return
        shell.query_output(output['data']['rows'], output['data']['columns'])
    except Exception as e:
        raise RuntimeError(f"{e}") from e

#command .create
def do_create(shell, arg):
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
    
    # Validate the database name
    if not re.match(r"^[A-Za-z0-9-]{6,50}$", database_name):
        utils.write_output("Error: Database name must be between 6 and 50 characters long and contain only letters, numbers, and hyphens.")
        return
    
    try:
        shell.edgeSql.create_database(database_name)
        utils.write_output(f"Database '{database_name}' created successfully.")
    except Exception as e:
        raise RuntimeError(f"{e}") from e

#command .destroy
def do_destroy(shell, arg):
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
    
    # Confirm the action
    confirmation = input(f"Are you sure you want to destroy the database '{database_name}'? This action cannot be undone. (yes/no): ")
    if confirmation.lower() != 'yes':
        utils.write_output("Database destruction aborted.")
        return

    try:
        status = shell.edgeSql.destroy_database(database_name)
        if status:
            utils.write_output(f"Database '{database_name}' has been successfully destroyed.")
        else:
            utils.write_output(f"Failed to destroy database '{database_name}'.")
    except Exception as e:
        raise RuntimeError(f"{e}") from e
