from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import os

class GoogleSheetsConnector:
    def __init__(self) -> None:
        pass

    def insert_rows_from_dicts(self, spreadsheet_id, ranger, list_of_dicts):
        """
        Inserts multiple rows into a Google Sheet using a service account, with headers determined by dictionary keys.
        
        Args:
        service_account_file (str): Path to the service account credential file.
        spreadsheet_id (str): The ID of the spreadsheet.
        range_name (str): The range to insert the rows in A1 notation (e.g., 'Sheet1!A1').
        list_of_dicts (list): A list of dictionaries, each representing a row to insert.
        
        Returns:
        dict: Response from the Sheets API.
        """
        if not list_of_dicts:
            raise ValueError("list_of_dicts is empty")

        # Authenticate using the service account
        service_account_info = {
            "type": os.environ["type"],
            "project_id": os.environ["project_id"],
            "private_key_id": os.environ["private_key_id"],
            "private_key": os.environ["private_key"],
            "client_email": os.environ["client_email"],
            "client_id": os.environ["client_id"],
            "auth_uri": os.environ["auth_uri"],
            "token_uri": os.environ["token_uri"],
            "auth_provider_x509_cert_url": os.environ["auth_provider_x509_cert_url"],
            "client_x509_cert_url": os.environ["client_x509_cert_url"],
            "universe_domain": os.environ["universe_domain"]
        }
        creds = Credentials.from_service_account_info(service_account_info, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        service = build('sheets', 'v4', credentials=creds)

        # Extract headers from the keys of the first dictionary
        headers = list(list_of_dicts[0].keys())
        # Prepare the values to insert (include headers if the sheet is initially empty or as needed)
        values = []  # Start with headers
        # Append each dictionary as a row of values
        for row_dict in list_of_dicts:
            row_values = [row_dict.get(header, '') for header in headers]  # Collect values in order of headers, fill missing with empty
            values.append(row_values)

        # Prepare the request body
        body = {
            'values': values
        }

        # Call the Sheets API to append data
        result = service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            valueInputOption='USER_ENTERED',
            insertDataOption='INSERT_ROWS',
            range=ranger,
            body=body
        ).execute()

        return result
    
    def update_row_from_dict(self, spreadsheet_id, ranger, list_of_dicts):
        
        if not list_of_dicts:
            raise ValueError("list_of_dicts is empty")

        # Authenticate using the service account
        service_account_info = {
            "type": os.environ["type"],
            "project_id": os.environ["project_id"],
            "private_key_id": os.environ["private_key_id"],
            "private_key": os.environ["private_key"],
            "client_email": os.environ["client_email"],
            "client_id": os.environ["client_id"],
            "auth_uri": os.environ["auth_uri"],
            "token_uri": os.environ["token_uri"],
            "auth_provider_x509_cert_url": os.environ["auth_provider_x509_cert_url"],
            "client_x509_cert_url": os.environ["client_x509_cert_url"],
            "universe_domain": os.environ["universe_domain"]
        }
        creds = Credentials.from_service_account_info(service_account_info, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        service = build('sheets', 'v4', credentials=creds)

        # Extract headers from the keys of the first dictionary
        headers = list(list_of_dicts[0].keys())
        # Prepare the values to insert (include headers if the sheet is initially empty or as needed)
        values = []  # Start with headers
        # Append each dictionary as a row of values
        for row_dict in list_of_dicts:
            row_values = [row_dict.get(header, '') for header in headers]  # Collect values in order of headers, fill missing with empty
            values.append(row_values)

        # Prepare the request body
        body = {
            'values': values
        }

        # Call the Sheets API to append data
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            valueInputOption='USER_ENTERED',
            range=ranger,
            body=body
        ).execute()

        return result

    def get_rows(self, spreadsheet_id, ranger):
        """
        Gets rows from a specified range in a Google Sheet using a service account.
        
        Args:
        spreadsheet_id (str): The ID of the spreadsheet.
        range_name (str): The range to retrieve the rows from in A1 notation (e.g., 'Sheet1!A1:D10').
        
        Returns:
        list: List of rows retrieved from the Sheets API.
        """
        # Authenticate using the service account
        service_account_info = {
            "type": os.environ["type"],
            "project_id": os.environ["project_id"],
            "private_key_id": os.environ["private_key_id"],
            "private_key": os.environ["private_key"],
            "client_email": os.environ["client_email"],
            "client_id": os.environ["client_id"],
            "auth_uri": os.environ["auth_uri"],
            "token_uri": os.environ["token_uri"],
            "auth_provider_x509_cert_url": os.environ["auth_provider_x509_cert_url"],
            "client_x509_cert_url": os.environ["client_x509_cert_url"],
            "universe_domain": os.environ["universe_domain"]
        }
        creds = Credentials.from_service_account_info(service_account_info, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        service = build('sheets', 'v4', credentials=creds)

        # Call the Sheets API to get data
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=ranger
        ).execute()

        rows = result.get('values', [])

        return rows
    
    def bulk_delete_rows(self, spreadsheet_id, sheet_id, start_index, end_index):

        # Authenticate using the service account
        service_account_info = {
            "type": os.environ["type"],
            "project_id": os.environ["project_id"],
            "private_key_id": os.environ["private_key_id"],
            "private_key": os.environ["private_key"],
            "client_email": os.environ["client_email"],
            "client_id": os.environ["client_id"],
            "auth_uri": os.environ["auth_uri"],
            "token_uri": os.environ["token_uri"],
            "auth_provider_x509_cert_url": os.environ["auth_provider_x509_cert_url"],
            "client_x509_cert_url": os.environ["client_x509_cert_url"],
            "universe_domain": os.environ["universe_domain"]
        }
        creds = Credentials.from_service_account_info(service_account_info, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        service = build('sheets', 'v4', credentials=creds)

        # Send the batch update request
        body = {
          "requests": [
            {
              "deleteDimension": {
                "range": {
                  "sheetId": sheet_id,
                  "dimension": "ROWS",
                  "startIndex": start_index,
                  "endIndex": end_index
                }
              }
            }
          ],
        }
        response = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()

        return response
    
    def insert_rows_from_dicts_v2(self, spreadsheet_id, ranger, list_of_dicts):
        """
        Inserts multiple rows into a Google Sheet using a service account, with headers determined by dictionary keys.
        
        Args:
        service_account_file (str): Path to the service account credential file.
        spreadsheet_id (str): The ID of the spreadsheet.
        range_name (str): The range to insert the rows in A1 notation (e.g., 'Sheet1!A1').
        list_of_dicts (list): A list of dictionaries, each representing a row to insert.
        
        Returns:
        dict: Response from the Sheets API.
        """
        if not list_of_dicts:
            raise ValueError("list_of_dicts is empty")

        # Authenticate using the service account
        service_account_info = {
            "type": os.environ["type"],
            "project_id": os.environ["project_id"],
            "private_key_id": os.environ["private_key_id"],
            "private_key": os.environ["private_key"],
            "client_email": os.environ["client_email"],
            "client_id": os.environ["client_id"],
            "auth_uri": os.environ["auth_uri"],
            "token_uri": os.environ["token_uri"],
            "auth_provider_x509_cert_url": os.environ["auth_provider_x509_cert_url"],
            "client_x509_cert_url": os.environ["client_x509_cert_url"],
            "universe_domain": os.environ["universe_domain"]
        }
        creds = Credentials.from_service_account_info(service_account_info, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        service = build('sheets', 'v4', credentials=creds)

        # Extract headers from the keys of the first dictionary
        headers = list(list_of_dicts[0].keys())
        # Prepare the values to insert (include headers if the sheet is initially empty or as needed)
        values = []  # Start with headers
        # Append each dictionary as a row of values
        for row_dict in list_of_dicts:
            row_values = [row_dict.get(header, '') for header in headers]  # Collect values in order of headers, fill missing with empty
            values.append(row_values)

        # Prepare the request body
        body = {
            'values': values
        }

        # Call the Sheets API to append data
        result = service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            valueInputOption='USER_ENTERED',
            insertDataOption='INSERT_ROWS',
            range=ranger,
            body=body
        ).execute()

        return result
    
    def clear_rows(self, spreadsheet_id, sheet_name):
        """
        Clears all rows starting from row 2 to the last row in the specified sheet.
        
        Args:
        spreadsheet_id (str): The ID of the spreadsheet.
        sheet_name (str): The name of the sheet to clear rows from.
        
        Returns:
        dict: Response from the Sheets API.
        """
        # Authenticate using the service account
        service_account_info = {
            "type": os.environ["type"],
            "project_id": os.environ["project_id"],
            "private_key_id": os.environ["private_key_id"],
            "private_key": os.environ["private_key"],
            "client_email": os.environ["client_email"],
            "client_id": os.environ["client_id"],
            "auth_uri": os.environ["auth_uri"],
            "token_uri": os.environ["token_uri"],
            "auth_provider_x509_cert_url": os.environ["auth_provider_x509_cert_url"],
            "client_x509_cert_url": os.environ["client_x509_cert_url"],
            "universe_domain": os.environ["universe_domain"]
        }
        creds = Credentials.from_service_account_info(service_account_info, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        service = build('sheets', 'v4', credentials=creds)

        # Define the range to clear (from row 2 onwards)
        clear_range = f"{sheet_name}!2:8000"  # You can set a large number for the row limit

        # Call the Sheets API to clear the range
        result = service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=clear_range,
            body={}
        ).execute()

        return result