import sys
from collections import deque

def write_output(message, destination='', mode='a'):
    """
    Writes a message to either stdout or a specified file.

    Args:
        message (str): The message to be written.
        destination (str or file object, optional): The destination for writing. Default is stdout.
        mode (str, optional): The mode for opening the file. Default is 'a' (append).

    Returns:
        None
    """
    try:
        if destination == '':
            print(message)
        else:
            with open(destination, mode, encoding='utf-8') as file:
                file.write(message+'\n')

    except OSError as e:
        print(f"Error writing message to file {destination}: {e}")


def contains_any(arg, substrings, case_sensitive=False):
    """
    Checks if the string contains any of the substrings provided.

    Args:
        arg (str): The string to be checked.
        substrings (list): A list of substrings to be checked.
        case_sensitive (bool, optional): Whether to perform case-sensitive matching. Default is False.

    Returns:
        bool: True if the string contains any of the substrings, False otherwise.
    """
    try:
        if case_sensitive:
            return any(substring in arg for substring in substrings)
        else:
            return any(substring.lower() in arg.lower() for substring in substrings)
    except TypeError as e:
        print(f"Error checking string for substrings: {e}")
        return False

def signal_handler(sig, frame):
    """Handle Ctrl+C signal."""
    write_output('\nCtrl+C pressed. Exiting EdgeSQL Shell.')
    sys.exit(0)

def total_size(obj, seen=None):
    """Recursively finds size of objects, accounting for contents."""
    seen = seen or set()
    size = 0
    objects = deque([obj])

    while objects:
        current = objects.popleft()
        if id(current) in seen:
            continue
        seen.add(id(current))
        size += sys.getsizeof(current)

        if isinstance(current, dict):
            objects.extend(current.keys())
            objects.extend(current.values())
        elif hasattr(current, '__dict__'):
            objects.append(current.__dict__)
        elif hasattr(current, '__iter__') and not isinstance(current, (str, bytes, bytearray)):
            objects.extend(current)
    
    return size

def get_size_of_chunk(chunk):
    """
    Calculate the size of a DataFrame chunk in bytes.

    Args:
        chunk (pandas.DataFrame): The DataFrame chunk.

    Returns:
        int: The size of the DataFrame chunk in bytes.
    """
    return chunk.memory_usage(index=True, deep=True).sum()
