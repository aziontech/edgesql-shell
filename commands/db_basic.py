import utils

def do_tables(shell, arg):
    """
    List all tables in the current database.

    Args:
        shell (EdgeSQLShell): The shell instance.
        arg (str): Additional arguments (not used).
    """
    try:
        output = shell.edgeSql.list_tables()
        if not output['success']:
            utils.write_output(f"Error: {output['error']}")
            return
    except RuntimeError as e:
        utils.write_output(f"Error: {e}")
        return

    if output['data'] and len(output['data']['rows']) > 0:
        shell.query_output(output['data']['rows'], output['data']['columns'])
    else:
        utils.write_output("No tables available.")

def do_schema(shell, arg):
    """
    Describe the schema of a table.

    Args:
        shell (EdgeSQLShell): The shell instance.
        arg (str): The name of the table to describe.
    """
    if not arg:
        utils.write_output("Usage: .schema <table_name>")
        return

    table_name = arg.strip()
    
    try:
        output = shell.edgeSql.describe_table(table_name)
        if not output['success']:
            utils.write_output(f"Error: {output['error']}")
            return
    except RuntimeError as e:
        utils.write_output(f"Error: {e}")
        return

    if output['data'] and len(output['data']['rows']) > 0:
        shell.query_output(output['data']['rows'], output['data']['columns'])
    else:
        utils.write_output(f"No schema information found for table '{table_name}'.")
