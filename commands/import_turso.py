import os
import requests
from http import HTTPStatus
import pandas as pd
from halo import Halo
import utils

def importer(db_name, source_table, chunksize=512, max_chunk_size_mb=0.8):
    """
    Import data from a Turso database in chunks.

    Args:
        db_name (str): The name of the database.
        source_table (str): The name of the source table to import data from.
        chunksize (int, optional): The maximum number of rows per chunk to return. Default is 512.
        max_chunk_size_mb (float, optional): The maximum size of each chunk in megabytes. Default is 0.8 MB.

    Yields:
        pandas.DataFrame: A chunk of the data from the source table.
    """
    base_url = os.getenv("TURSO_DATABASE_URL")
    auth_token = os.getenv("TURSO_AUTH_TOKEN")
    max_chunk_size_bytes = max_chunk_size_mb * 1024 * 1024

    if not all([base_url, auth_token]):
        raise EnvironmentError(f"{db_name.upper()} environmental variables not set correctly.")

    url = f'{base_url}/v2/pipeline'

    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }

    # Get total number of rows for progress tracking
    total_rows = 0
    try:
        request_body = {
            "requests": [
                {
                    "type": "execute",
                    "stmt": {
                        "sql": f"SELECT COUNT(*) AS total FROM {source_table}"
                    }
                }
            ]
        }
        response = requests.post(url, headers=headers, json=request_body, timeout=30)
        response.raise_for_status()
        json_data = response.json()
        if response.status_code == HTTPStatus.OK:  # 200
            results = json_data.get('results', [])
            if results:
                response_data = results[0].get('response', {})
                if response_data:
                    result_data = response_data.get('result', {})
                    if result_data:
                        rows = result_data.get('rows', [])
                        if rows:
                            total_rows = int(rows[0][0]['value'])  # Assuming the count is in the first row, first column
    except requests.exceptions.RequestException as e:
        raise requests.exceptions.RequestException(f"Error during {db_name} total rows query: {e}") from e

    # Yield chunks of data
    offset = 0
    spinner = Halo(text='Analyzing dataset and calculating chunks...', spinner='line')
    spinner.start()

    try:
        while True:
            current_chunk_size = 0
            chunk_rows = []
            columns = []

            while current_chunk_size < max_chunk_size_bytes and len(chunk_rows) < chunksize:
                try:
                    # Calculate the limit based on remaining rows and maximum chunk rows
                    limit = chunksize - len(chunk_rows)

                    request_body = {
                        "requests": [
                            {
                                "type": "execute",
                                "stmt": {
                                    "sql": f"SELECT * FROM {source_table} LIMIT {limit} OFFSET {offset}"
                                }
                            },
                            {
                                "type": "close"
                            }
                        ]
                    }

                    response = requests.post(url, headers=headers, json=request_body, timeout=30)
                    response.raise_for_status()
                    json_data = response.json()
                    if response.status_code == HTTPStatus.OK:  # 200
                        results = json_data.get('results', [])
                        if results:
                            response_data = results[0].get('response', {})
                            if response_data:
                                result_data = response_data.get('result', {})
                                if result_data:
                                    columns = [col['name'] for col in result_data.get('cols', [])]
                                    rows = [[item['value'] for item in row] for row in result_data.get('rows', [])]
                                    if rows:
                                        df_row = pd.DataFrame(rows, columns=columns)
                                        row_size = utils.get_size_of_chunk(df_row)
                                        
                                        if current_chunk_size + row_size > max_chunk_size_bytes:
                                            break
                                        
                                        chunk_rows.extend(rows)
                                        current_chunk_size += row_size
                                        offset += len(rows)  # Move offset by the number of rows fetched
                                    else:
                                        break
                                else:
                                    break
                        else:
                            break
                    else:
                        break
                except requests.exceptions.RequestException as e:
                    raise requests.exceptions.RequestException(f"Error during {db_name} data fetch: {e}") from e

            if not chunk_rows:
                break

            df_chunk = pd.DataFrame(chunk_rows, columns=columns)
            yield df_chunk

        spinner.succeed('Data import completed!')
    except Exception as e:
        spinner.fail('Error during data analysis!')
        raise e
    except KeyboardInterrupt:
        spinner.stop()
        print("Data analysis interrupted!")
