import utils
import utils_sql as sql
import pandas as pd

DUMP_SCHEMA_ONLY = 0x1
DUMP_DATA_ONLY = 0x1 << 1
DUMP_ALL = DUMP_SCHEMA_ONLY | DUMP_DATA_ONLY
DUMP_NONE = 0x0

def dump_table(shell, table_name, dump=DUMP_ALL, batch_size=512):
    """
    Dump table structure and data as SQL.

    Args:
        table_name (str): Name of the table to dump.
        dump (int, optional): Flag indicating what to dump (schema only, data only, or both). Defaults to DUMP_ALL.
        batch_size (int, optional): Size of each data batch for fetching. Defaults to 1000.
    """
    try:
        # Check if the table exists
        if not shell.edgeSql.exist_table(table_name):
            utils.write_output(f"Table '{table_name}' not found", shell.output)
            return

        # Dump schema if requested
        if dump & DUMP_SCHEMA_ONLY:
            # Table structure
            table_output = shell.edgeSql.execute(f"SELECT sql FROM sqlite_schema WHERE type like 'table' \
                                    AND sql NOT NULL \
                                    AND name like '{table_name}' \
                                    ORDER BY tbl_name='sqlite_sequence', rowid;")
            if table_output:
                statement = table_output['rows'][0][0]
                formatted_query = sql.format_sql(f'CREATE TABLE IF NOT EXISTS {statement[13:]};')
                utils.write_output(formatted_query, shell.output)
            
            # Indexes, Triggers, and Views
            additional_objects_output = shell.edgeSql.execute(f"SELECT sql FROM sqlite_schema \
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
                        utils.write_output(formatted_query, shell.output)

        # Dump data if requested
        if dump & DUMP_DATA_ONLY:
            # Get total count of rows in the table
            count_output = shell.edgeSql.execute(f'SELECT COUNT(*) FROM {table_name};')
            if count_output and count_output['rows']:
                total_rows = count_output['rows'][0][0]

                # Fetch data in batches and generate SQL insert statements
                offset = 0
                while offset < total_rows:
                    limit = min(batch_size, total_rows - offset)  # Calculate limit for this batch
                    data_output = shell.edgeSql.execute(f'SELECT * FROM {table_name} LIMIT {limit} OFFSET {offset};')
                    if data_output:
                        df = pd.DataFrame(data_output['rows'], columns=data_output['columns'])
                        sql_commands = sql.generate_insert_sql(df, table_name)
                        for cmd in sql_commands:
                            utils.write_output(cmd, shell.output)
                    
                    offset += batch_size

    except Exception as e:
        raise RuntimeError(f"Error dumping table '{table_name}': {e}") from e

    
def _dump(shell, arg=False, dump=DUMP_ALL):
    """
    Dump database structure and data as SQL.

    Args:
        arg (list, optional): List of specific tables to dump. Defaults to False.
        dump (int, optional): Flag indicating what to dump (schema only, data only, or both). Defaults to DUMP_ALL.
    """
    try:
        utils.write_output("PRAGMA foreign_keys=OFF;", shell.output)
        utils.write_output("BEGIN TRANSACTION;", shell.output)

        # Dump all tables if no specific tables are provided
        if not arg or len(arg) == 0:
            tables_output = shell.edgeSql.execute("SELECT name FROM sqlite_schema WHERE type like 'table';")
            if tables_output:
                table_lst = tables_output['rows']
                for table in table_lst:
                    table_name = table[0]
                    if table_name == "sqlite_sequence":
                        utils.write_output("DELETE FROM sqlite_sequence;", shell.output)
                    elif table_name == "sqlite_stat1":
                        utils.write_output("ANALYZE sqlite_master;", shell.output)
                    elif table_name.startswith("sqlite_"):
                        continue
                    else:
                        dump_table(shell, table_name, dump)
        else: # Dump particular table(s)
            for tbl in arg:
                dump_table(shell, tbl, dump)

        utils.write_output("COMMIT;", shell.output)
        utils.write_output("PRAGMA foreign_keys=ON;", shell.output)

    except Exception as e:
        raise RuntimeError(f"Error dumping database: {e}") from e


def do_dump(shell, arg):
    """
    Render database structure as SQL.

    Args:
        arg (str): Optional arguments '--schema-only', '--data-only', or table name(s).
    """
    if not shell.edgeSql.get_current_database_id():
        utils.write_output("No database selected. Use '.use <database_name>' to select a database.")
        return

    dump_type = DUMP_NONE

    if not arg:
        _dump(shell)
    else:
        args = arg.split()

        if '--schema-only' in args:
            args.remove('--schema-only')
            dump_type = dump_type | DUMP_SCHEMA_ONLY
        if '--data-only' in args:
            args.remove('--data-only')
            dump_type = dump_type | DUMP_DATA_ONLY

        if dump_type == DUMP_NONE:
            _dump(shell, arg=args, dump=DUMP_ALL)
        else:
            _dump(shell, arg=args,dump=dump_type)