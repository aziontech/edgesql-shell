import os
import cmd
import sys
import utils
import edgesql
from tabulate import tabulate
import signal
from pathlib import Path
from pathvalidate import ValidationError, validate_filepath
import pandas as pd
from io import StringIO
import importlib.util


IGNORE_TOKENS = ['--']


class EdgeSQLShell(cmd.Cmd):
    def __init__(self, edgesql_instance):
        """
        Initialize EdgeSQLShell object.

        Args:
            edgesql (EdgeSQL): An instance of the EdgeSQL class.
        """
        super().__init__()
        self.edgeSql = edgesql_instance
        self.prompt = 'EdgeSQL'
        self.update_prompt()
        self.last_command = ''
        self.multiline_command = []
        self.output = ''
        self.outFormat = 'tabular'
        self.transaction = False
        self.command_mapping = self.__command_map()

        # Import and register commands from commands directory
        self.import_commands_from_directory("commands")

    def __command_map(self):
        """Return a dictionary mapping shell commands to corresponding methods."""
        command_mapping = {
            ".exit": self.do_exit,
            ".output": self.do_output,
            ".mode": self.do_mode,
        }
        return command_mapping

    def import_commands_from_directory(self, directory):
        """Dynamically import and register commands from a specified directory."""
        for file_name in os.listdir(directory):
            if file_name.endswith(".py") and file_name != "__init__.py":
                module_name = file_name[:-3]  # Remove .py extension
                self.import_and_register_module(module_name, os.path.join(directory, file_name))

    def import_and_register_module(self, module_name, file_path):
        """Import and register commands from a specified module."""
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        custom_module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = custom_module
        spec.loader.exec_module(custom_module)

        method_names = [method for method in dir(custom_module) if callable(getattr(custom_module, method))]
        self.import_and_register_commands(module_name, method_names)

    def import_and_register_commands(self, module_name, method_names):
        """Import and register commands from a module."""
        for method_name in method_names:
            if method_name.startswith("do_"):
                command_name = method_name[3:]
                command_func = getattr(sys.modules[module_name], method_name)
                self.add_command(command_name, command_func)

    def add_command(self, command_name, method):
        """Add a new command to the command mapping."""
        setattr(self, f"do_{command_name}", method.__get__(self))
        self.command_mapping[f".{command_name}"] = method.__get__(self)

    def get_all_commands(self, text):
        """Get all commands including dynamically loaded commands."""
        available_commands = list(self.command_mapping.keys())
        if text:
            return [cmd for cmd in available_commands if cmd.startswith(text)]
        else:
            return available_commands
    
    def completenames(self, text, *ignored):
        """Tab-completion for all commands."""
        return self.get_all_commands(text)

    def complete_help(self, text, line, begidx, endidx):
        """Tab-completion for the help command."""
        return self.get_all_commands(text)

    def do_help(self, arg):
        """
        Display help for available commands.

        Args:
            arg (str): Optional argument. If provided, displays detailed help for the specified command.

        Examples:
            help: Display the list of available commands.
            help <command>: Display detailed help for the specified command.
        """
        if arg:
            command = arg.strip()
            if command in self.command_mapping:
                method_name = self.command_mapping[command].__name__
                docstring = getattr(self, method_name).__doc__
                if docstring:
                    print(f"{command}: {docstring.strip()}")
                else:
                    print(f"{command}: No help available")
            else:
                print(f"Unknown command: {command}")
        else:
            command_list = sorted(self.command_mapping.keys())
            print("Documented commands (type help <topic>):")
            print("=" * 40)
            super().columnize(command_list)
            print()

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

    def do_output(self, arg):
        """
        Set the output to stdout or file.

        Args:
            arg (str): 'stdout' to output to console, or a file path.
        """
        if not arg:
            utils.write_output("Usage: .output stdout|file_path")
            return
        
        try:
            output_mode = arg.split()[0].lower()
            if output_mode == 'stdout':
                self.output = ''
                utils.write_output("Output set to stdout.")
            else:
                file_path = Path(output_mode)
                validate_filepath(file_path, platform='auto')
                self.output = output_mode
                utils.write_output(f"Output set to file: {output_mode}")
        except ValidationError as er:
            raise ValidationError(f"Error: {er}") from er

    def do_mode(self, arg):
        """
        Set output mode.

        Args:
            arg (str): Output mode ('excel', 'tabular', 'csv', 'json','html', 'markdown', 'raw').
        """
        mode_lst = ['excel', 'tabular','csv','json','html','markdown','raw']
        arg_lower = arg.lower() if arg else None
    
        if not arg_lower or arg_lower not in mode_lst:
            utils.write_output("Usage: .mode excel|tabular|csv|json|html|markdown|raw")
            return

        self.outFormat = arg_lower
        utils.write_output(f"Output mode set to: {arg_lower}")

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
        except Exception as er:
            raise RuntimeError(f"Error executing SQL command: {er}") from er

        self.multiline_command = []  # Reset multiline command buffer

    def default(self, line):
        """Execute SQL command and handle multiline input."""
        try:
            arg = line.strip()
            if arg:
                # New command
                self.last_command = line
            
                # Check if the command starts with a dot (indicating a shell command)
                if arg.startswith("."):
                    command, *args = arg.split(" ")
                    command_map = self.command_mapping  # Get the command mapping
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
                        try: 
                            output = self.edgeSql.execute(arg)
                        except RuntimeError as er:
                            utils.write_output(f"Error executing SQL command: {er}")
                            output = None
                        if output:
                            self.query_output(output['rows'], output['columns'])
                        return
                    else:
                        # Multi-line command, accumulate lines
                        self.multiline_command.append(arg)
            else:
                pass
        except RuntimeError as er:
            utils.write_output(f"{er}", '')

    def query_output(self, rows, columns):
        """Format and output query results."""
        df = pd.DataFrame(rows, columns=columns)

        def output_to_buffer(format_func, *args, **kwargs):
            buffer = StringIO()
            format_func(df, buffer, *args, **kwargs)
            buffer.seek(0)
            utils.write_output(buffer.getvalue(), self.output)

        format_map = {
            'tabular': lambda df: tabulate(df.to_dict(orient='records'), headers="keys", tablefmt='fancy_grid'),
            'markdown': lambda df: tabulate(df.to_dict(orient='records'), headers="keys", tablefmt='pipe'),
            'csv': lambda df, buffer: df.to_csv(buffer, index=False),
            'json': lambda df, buffer: df.to_json(buffer, index=False, orient='records', indent=4),
            'html': lambda df, buffer: df.to_html(buffer, index=False),
            'raw': lambda df, buffer: df.to_csv(buffer, sep=' ', index=False),
        }

        if self.outFormat not in format_map and self.outFormat != 'excel':
            raise ValueError(f"Unsupported output format: {self.outFormat}")

        try:
            if self.outFormat == 'excel' and not self.output.endswith(".xlsx"):
                utils.write_output('For "excel" mode, the output file must have the extension .xlsx', '')
            elif self.output == '':
                if self.outFormat in ['csv', 'json', 'html', 'raw']:
                    output_to_buffer(format_map[self.outFormat])
                elif self.outFormat in ['tabular', 'markdown']:
                    formatted_data = format_map[self.outFormat](df)
                    utils.write_output(formatted_data, self.output)
                elif self.outFormat == 'excel':
                    buffer = StringIO()
                    df.to_excel(buffer, index=False)
                    buffer.seek(0)
                    utils.write_output(buffer.getvalue(), self.output)
            else:
                if self.outFormat in ['csv', 'json', 'html', 'raw']:
                    with open(self.output, 'w', encoding="utf-8") as f:
                        format_map[self.outFormat](df, f)
                elif self.outFormat in ['tabular', 'markdown']:
                    formatted_data = format_map[self.outFormat](df)
                    utils.write_output(formatted_data, self.output)
                elif self.outFormat == 'excel':
                    df.to_excel(self.output, index=False)
        except Exception as er:
            raise RuntimeError(f"An error occurred: {er}", '') from er


        def execute_commands(self, cmds):
            """Execute a list of commands."""
            for command in cmds:    
                if command.startswith("."):
                    command_parts = command.split()
                    command_name = command_parts[0]
                    if command_name in self.command_mapping:
                        args = ' '.join(command_parts[1:])
                        self.command_mapping[command_name](args)
                else:
                    self.execute_sql_command(command)

    def execute_sql_command(self, sql_command):
        """Execute a SQL command."""
        try:
            output = self.edgeSql.execute(sql_command)
            if output:
                self.query_output(output['rows'], output['columns'])
        except Exception as er:
            raise RuntimeError(f"Error executing SQL command: {er}") from er

    def process_arguments(self, arguments):
        """Process command-line arguments."""
        interactive = True
        cmds = []
        skip_next = False  # Flag to skip processing the next argument if it's a command
        for idx, arg in enumerate(arguments):
            if skip_next:
                skip_next = False
                continue

            if arg == "-n":
                interactive = False
            elif arg.startswith("-c"):
                cmds = arg[2:]  # Get the command after '-c'
                # If the next argument exists and is not another option
                if idx + 1 < len(arguments) and not arguments[idx + 1].startswith("-"):
                    command += arguments[idx + 1]  # Append the next argument as part of the command
                    skip_next = True  # Skip processing the next argument since it's part of the command
                cmds.append(cmds.strip())
            elif arg in ["-h", "--help"]:
                utils.write_output(
                    """
                    Usage:
                    -n: Non-interactive mode.
                    -c <command>: Execute a command.
                    -h, --help: Show this help message.
                    """
                )
                sys.exit()
        return interactive, cmds


if __name__ == "__main__":
    signal.signal(signal.SIGINT, utils.signal_handler)
    token = os.environ.get('AZION_TOKEN')
    if token is None:
        utils.write_output("Authorization token not found in environment variable AZION_TOKEN")
        exit(1)

    base_url = os.environ.get('AZION_BASE_URL')
    edgSql = edgesql.EdgeSQL(token, base_url)
    azion_db_shell = EdgeSQLShell(edgSql)
    
    # Process command-line arguments
    interactive_mode, commands = azion_db_shell.process_arguments(sys.argv[1:])

    if interactive_mode:
        azion_db_shell.cmdloop("Welcome to EdgeSQL Shell. Type '.exit' to quit.")
    else:
        # Execute commands non-interactively
        try:
            azion_db_shell.execute_commands(commands)
        except RuntimeError as e:
            utils.write_output(f"Error executing SQL command: {e}")
