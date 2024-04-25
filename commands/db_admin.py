import utils
from tabulate import tabulate

#command databases
def do_databases(shell, arg):
    """List all databases."""
    db_list = shell.edgeSql.list_databases()
    if db_list:
        databases = db_list.get('databases')
        columns = db_list.get('columns')
        if databases and columns:
            formatted_table = tabulate(databases, headers=columns, tablefmt="fancy_grid")
            utils.write_output(formatted_table, shell.output)
        else:
            utils.write_output("Error: Invalid database information.")
    else:
        utils.write_output("No databases found.")

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

    if shell.edgeSql.set_current_database(database_name):
        shell.update_prompt()
        utils.write_output(f"Switched to database '{arg}'.")

#comand .dbinfo
def do_dbinfo(shell, arg):
    """Get information about the current database."""
    if not shell.edgeSql.get_current_database_id():
        utils.write_output("No database selected. Use '.use <database_name>' to select a database.")
        return

    db_info = shell.edgeSql.get_database_info()
    if db_info:
        data = db_info.get('table_data')
        columns = db_info.get('columns')
        database_info = tabulate(data, headers=columns, tablefmt="fancy_grid")
        utils.write_output(database_info, shell.output)
    else:
        utils.write_output("Error: Unable to fetch database information.")

#Command .create

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
    
    try:
        shell.edgeSql.create_database(database_name)
    except Exception as e:
        utils.write_output(f"Error creating database: {e}")

#Command .destroy
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

    try:
        database_id = shell.edgeSql.get_database_id(database_name)
        if not database_id:
            utils.write_output(f"Database '{database_name}' not found.")
            return
        
        shell.edgeSql.destroy_database(database_id)
    except Exception as e:
        utils.write_output(f"Error destroying database: {e}")