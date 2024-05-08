import utils
import utils_sql as sql

def do_tables(shell, arg):
    """List all tables."""
    try:
        output = shell.edgeSql.list_tables()
    except Exception as e:
        utils.write_output(f"Error: {e}")
        return

    if output is not None:
        rows = output.get('rows')
        columns = output.get('columns')
        if rows and columns:
            shell.query_output(rows, columns)
        else:
            utils.write_output("No tables available.")
    else:
        utils.write_output("Error listing tables.")


def do_schema(shell, arg):
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
    
    try:
        output = shell.edgeSql.describe_table(table_name)
    except Exception as e:
        utils.write_output(f"Error: {e}")
        return

    if output is not None:
        rows = output.get('rows')
        columns = output.get('columns')
        if rows and columns:
            shell.query_output(rows, columns)
        else:
            utils.write_output(f"No schema information found for table '{table_name}'.")
    else:
        utils.write_output("Error describing table schema.")