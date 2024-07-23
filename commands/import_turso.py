import os
import requests
from http import HTTPStatus
import pandas as pd

def importer(db_name, source_table):

    base_url = os.getenv("TURSO_DATABASE_URL")
    auth_token = os.getenv("TURSO_AUTH_TOKEN")
    #encryption_key = os.getenv("TURSO_ENCRYPTION_KEY")

    if not all([base_url, auth_token]):
        raise EnvironmentError(f"{db_name.upper()} environmental variables not set correctly.")

    url = f'{base_url}/v2/pipeline'

    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }

    request_body = {
        "requests": [
            {
                "type": "execute",
                "stmt": {
                    "sql": f"SELECT * FROM {source_table}"
                }
            },
            {
                "type": "close"
            }
        ]
    }

    try:
        response = requests.post(url, headers=headers, json=request_body, timeout=30)
        response.raise_for_status()  # Raise an exception for 4XX and 5XX status codes
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
                        # Import data into a Pandas DataFrame
                        return pd.DataFrame(rows, columns=columns)
    except requests.exceptions.RequestException as e:
        raise requests.exceptions.RequestException(f"Error during {db_name} import: {e}") from e