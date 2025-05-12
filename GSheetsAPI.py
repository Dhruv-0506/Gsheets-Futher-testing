from flask import Flask, jsonify, request
import os
import requests
import logging
import time
import inspect # Added for introspection of function parameters

# Imports for Google API
from google.oauth2.credentials import Credentials as OAuthCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(module)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# --- Configuration (Main App) ---
CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID_FLASK_APP", "26763482887-coiufpukc1l69aaulaiov5o0u3en2del.apps.googleusercontent.com")
CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET_FLASK_APP", "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") # !!! STORE SECRET SECURELY !!!
TOKEN_URL = "https://oauth2.googleapis.com/token"
REDIRECT_URI = os.environ.get("GOOGLE_REDIRECT_URI_FLASK_APP", "https://serverless.on-demand.io/apps/googlesheets/auth/callback")
REQUEST_TIMEOUT_SECONDS = 30

# --- OAuth and Token Helper Functions (Main App) ---
def exchange_code_for_tokens(authorization_code):
    logger.info(f"Attempting to exchange authorization code for tokens. Code starts with: {authorization_code[:10]}...")
    start_time = time.time()
    if not CLIENT_SECRET:
        logger.error("CRITICAL: CLIENT_SECRET not configured for token exchange.")
        raise ValueError("CLIENT_SECRET not configured.")
    payload = {
        "code": authorization_code, "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI, "grant_type": "authorization_code"
    }
    try:
        response = requests.post(TOKEN_URL, data=payload, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        token_data = response.json()
        duration = time.time() - start_time
        if token_data.get("access_token"):
            logger.info(f"Successfully exchanged code for tokens in {duration:.2f} seconds.")
            return token_data
        else:
            logger.error(f"Token exchange response missing access_token after {duration:.2f}s. Response: {token_data}")
            raise ValueError("Access token not found in response.")
    except requests.exceptions.Timeout:
        duration = time.time() - start_time; logger.error(f"Timeout ({REQUEST_TIMEOUT_SECONDS}s) during token exchange after {duration:.2f} seconds."); raise
    except requests.exceptions.HTTPError as e:
        duration = time.time() - start_time; logger.error(f"HTTPError ({e.response.status_code}) during token exchange after {duration:.2f} seconds: {e.response.text if e.response else str(e)}"); raise
    except Exception as e:
        duration = time.time() - start_time; logger.error(f"Generic exception during token exchange after {duration:.2f} seconds: {str(e)}", exc_info=True); raise

def get_access_token(refresh_token):
    logger.info(f"Attempting to get new access token using refresh token (starts with: {refresh_token[:10]}...).")
    start_time = time.time()
    if not CLIENT_SECRET:
        logger.error("CRITICAL: CLIENT_SECRET not configured for token refresh.")
        raise ValueError("CLIENT_SECRET not configured.")
    payload = {
        "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET,
        "refresh_token": refresh_token, "grant_type": "refresh_token"
    }
    try:
        response = requests.post(TOKEN_URL, data=payload, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        token_data = response.json()
        access_token = token_data.get("access_token")
        duration = time.time() - start_time
        if access_token:
            logger.info(f"Successfully obtained new access token via refresh in {duration:.2f} seconds. Expires in: {token_data.get('expires_in')}s")
            return access_token
        else:
            logger.error(f"Token refresh response missing access_token after {duration:.2f}s. Response: {token_data}")
            raise ValueError("Access token not found in refresh response.")
    except requests.exceptions.Timeout:
        duration = time.time() - start_time; logger.error(f"Timeout ({REQUEST_TIMEOUT_SECONDS}s) during token refresh after {duration:.2f} seconds."); raise
    except requests.exceptions.HTTPError as e:
        duration = time.time() - start_time; logger.error(f"HTTPError ({e.response.status_code}) during token refresh after {duration:.2f} seconds: {e.response.text if e.response else str(e)}")
        if e.response and "invalid_grant" in (e.response.text or ""):
            logger.warning("Token refresh failed with 'invalid_grant'. Refresh token may be expired or revoked.")
        raise
    except Exception as e:
        duration = time.time() - start_time; logger.error(f"Generic exception during token refresh after {duration:.2f} seconds: {str(e)}", exc_info=True); raise

def get_sheets_service(access_token):
    logger.info("Building Google Sheets API service object...")
    if not access_token:
        logger.error("Cannot build sheets service: access_token is missing.")
        raise ValueError("Access token is required to build sheets service.")
    try:
        creds = OAuthCredentials(token=access_token)
        service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        logger.info("Google Sheets API service object built successfully.")
        return service
    except Exception as e:
        logger.error(f"Failed to build Google Sheets API service object: {str(e)}", exc_info=True); raise

# --- Central Google Sheets API Batch Update Function ---
def api_batch_update(service, spreadsheet_id, requests_list):
    if not requests_list:
        logger.warning("API: api_batch_update called with an empty requests_list.")
        return {"message": "No requests provided for batch update."}
    logger.info(f"API: Performing batchUpdate on sheet '{spreadsheet_id}' with {len(requests_list)} requests.")
    logger.debug(f"API: Batch update request body: {{'requests': {requests_list}}}")
    start_time = time.time()
    try:
        body = {"requests": requests_list}
        result = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
        duration = time.time() - start_time; logger.info(f"API: Batch update successful in {duration:.2f}s."); logger.debug(f"API: Batch update result: {result}"); return result
    except HttpError as e: duration = time.time() - start_time; error_content = e.content.decode('utf-8') if e.content else str(e); logger.error(f"API: HttpError during batchUpdate after {duration:.2f}s: {error_content}", exc_info=True); raise
    except Exception as e: duration = time.time() - start_time; logger.error(f"API: Generic error during batchUpdate after {duration:.2f}s: {str(e)}", exc_info=True); raise

# --- Google Sheets values.* API Wrapper Functions ---
def api_get_values(service, spreadsheet_id, range_name, major_dimension="ROWS", value_render_option="FORMATTED_VALUE", date_time_render_option="SERIAL_NUMBER"):
    logger.info(f"API: Getting values from sheet '{spreadsheet_id}', range '{range_name}'.")
    start_time = time.time()
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=range_name, majorDimension=major_dimension,
            valueRenderOption=value_render_option, dateTimeRenderOption=date_time_render_option
        ).execute()
        duration = time.time() - start_time; logger.info(f"API: Get values successful in {duration:.2f}s."); return result
    except HttpError as e: duration = time.time() - start_time; error_content = e.content.decode('utf-8') if e.content else str(e); logger.error(f"API: HttpError getting values after {duration:.2f}s: {error_content}", exc_info=True); raise
    except Exception as e: duration = time.time() - start_time; logger.error(f"API: Generic error getting values after {duration:.2f}s: {str(e)}", exc_info=True); raise

def api_batch_get_values(service, spreadsheet_id, ranges_list, major_dimension="ROWS", value_render_option="FORMATTED_VALUE", date_time_render_option="SERIAL_NUMBER"):
    logger.info(f"API: Batch getting values from sheet '{spreadsheet_id}', ranges: {ranges_list}.")
    start_time = time.time()
    try:
        result = service.spreadsheets().values().batchGet(
            spreadsheetId=spreadsheet_id, ranges=ranges_list, majorDimension=major_dimension,
            valueRenderOption=value_render_option, dateTimeRenderOption=date_time_render_option
        ).execute()
        duration = time.time() - start_time; logger.info(f"API: Batch get values successful in {duration:.2f}s."); return result
    except HttpError as e: duration = time.time() - start_time; error_content = e.content.decode('utf-8') if e.content else str(e); logger.error(f"API: HttpError batch getting values after {duration:.2f}s: {error_content}", exc_info=True); raise
    except Exception as e: duration = time.time() - start_time; logger.error(f"API: Generic error batch getting values after {duration:.2f}s: {str(e)}", exc_info=True); raise

def api_update_values(service, spreadsheet_id, range_name, values_data, value_input_option="USER_ENTERED"):
    logger.info(f"API: Updating values '{range_name}' in sheet '{spreadsheet_id}' with option '{value_input_option}'.")
    start_time = time.time()
    try:
        body = {"values": values_data} # values_data should be list of lists
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=range_name, valueInputOption=value_input_option, body=body
        ).execute()
        duration = time.time() - start_time; logger.info(f"API: Update values successful in {duration:.2f}s. Result: {result}"); return result
    except HttpError as e: duration = time.time() - start_time; error_content = e.content.decode('utf-8') if e.content else str(e); logger.error(f"API: HttpError updating values after {duration:.2f}s: {error_content}", exc_info=True); raise
    except Exception as e: duration = time.time() - start_time; logger.error(f"API: Generic error updating values after {duration:.2f}s: {str(e)}", exc_info=True); raise

def api_batch_update_values(service, spreadsheet_id, data_list, value_input_option="USER_ENTERED"):
    # data_list is a list of ValueRange objects e.g. [{"range": "A1", "values": [["Hello"]]}]
    logger.info(f"API: Batch updating values in sheet '{spreadsheet_id}' with option '{value_input_option}'. Batches: {len(data_list)}")
    start_time = time.time()
    try:
        body = {"valueInputOption": value_input_option, "data": data_list}
        result = service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
        duration = time.time() - start_time; logger.info(f"API: Batch update values successful in {duration:.2f}s."); return result
    except HttpError as e: duration = time.time() - start_time; error_content = e.content.decode('utf-8') if e.content else str(e); logger.error(f"API: HttpError batch updating values after {duration:.2f}s: {error_content}", exc_info=True); raise
    except Exception as e: duration = time.time() - start_time; logger.error(f"API: Generic error batch updating values after {duration:.2f}s: {str(e)}", exc_info=True); raise

def api_batch_update_values_by_data_filter(service, spreadsheet_id, data_filter_value_range_list, value_input_option="USER_ENTERED", include_values_in_response=False, response_value_render_option="FORMATTED_VALUE", response_date_time_render_option="SERIAL_NUMBER"):
    # data_filter_value_range_list is a list of DataFilterValueRange objects
    logger.info(f"API: Batch updating values by data filter in sheet '{spreadsheet_id}'.")
    start_time = time.time()
    try:
        body = {
            "valueInputOption": value_input_option,
            "data": data_filter_value_range_list,
            "includeValuesInResponse": include_values_in_response,
            "responseValueRenderOption": response_value_render_option,
            "responseDateTimeRenderOption": response_date_time_render_option
        }
        result = service.spreadsheets().values().batchUpdateByDataFilter(spreadsheetId=spreadsheet_id, body=body).execute()
        duration = time.time() - start_time; logger.info(f"API: Batch update values by data filter successful in {duration:.2f}s."); return result
    except HttpError as e: duration = time.time() - start_time; error_content = e.content.decode('utf-8') if e.content else str(e); logger.error(f"API: HttpError batch updating by data filter after {duration:.2f}s: {error_content}", exc_info=True); raise
    except Exception as e: duration = time.time() - start_time; logger.error(f"API: Generic error batch updating by data filter after {duration:.2f}s: {str(e)}", exc_info=True); raise

def api_append_values(service, spreadsheet_id, range_name, values_data, value_input_option="USER_ENTERED", insert_data_option="INSERT_ROWS", include_values_in_response=False, response_value_render_option="FORMATTED_VALUE", response_date_time_render_option="SERIAL_NUMBER"):
    logger.info(f"API: Appending values to sheet '{spreadsheet_id}', range '{range_name}', option '{value_input_option}'. Rows: {len(values_data)}")
    start_time = time.time()
    try:
        body = {"values": values_data}
        result = service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id, range=range_name, valueInputOption=value_input_option,
            insertDataOption=insert_data_option, body=body,
            includeValuesInResponse=include_values_in_response,
            responseValueRenderOption=response_value_render_option,
            responseDateTimeRenderOption=response_date_time_render_option
        ).execute()
        duration = time.time() - start_time; logger.info(f"API: Value append successful in {duration:.2f}s. Updates: {result.get('updates')}"); return result
    except HttpError as e: duration = time.time() - start_time; error_content = e.content.decode('utf-8') if e.content else str(e); logger.error(f"API: HttpError appending values after {duration:.2f}s: {error_content}", exc_info=True); raise
    except Exception as e: duration = time.time() - start_time; logger.error(f"API: Generic error appending values after {duration:.2f}s: {str(e)}", exc_info=True); raise

def api_clear_values(service, spreadsheet_id, range_name):
    logger.info(f"API: Clearing values from sheet '{spreadsheet_id}', range '{range_name}'.")
    start_time = time.time()
    try:
        result = service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=range_name, body={}).execute()
        duration = time.time() - start_time; logger.info(f"API: Values clear successful in {duration:.2f}s. Cleared range: {result.get('clearedRange')}"); return result
    except HttpError as e: duration = time.time() - start_time; error_content = e.content.decode('utf-8') if e.content else str(e); logger.error(f"API: HttpError clearing values after {duration:.2f}s: {error_content}", exc_info=True); raise
    except Exception as e: duration = time.time() - start_time; logger.error(f"API: Generic error clearing values after {duration:.2f}s: {str(e)}", exc_info=True); raise

def api_batch_clear_values(service, spreadsheet_id, ranges_list):
    # ranges_list is a list of A1 notation strings
    logger.info(f"API: Batch clearing values from sheet '{spreadsheet_id}', ranges: {ranges_list}.")
    start_time = time.time()
    try:
        body = {"ranges": ranges_list}
        result = service.spreadsheets().values().batchClear(spreadsheetId=spreadsheet_id, body=body).execute()
        duration = time.time() - start_time; logger.info(f"API: Batch clear values successful in {duration:.2f}s."); return result
    except HttpError as e: duration = time.time() - start_time; error_content = e.content.decode('utf-8') if e.content else str(e); logger.error(f"API: HttpError batch clearing values after {duration:.2f}s: {error_content}", exc_info=True); raise
    except Exception as e: duration = time.time() - start_time; logger.error(f"API: Generic error batch clearing values after {duration:.2f}s: {str(e)}", exc_info=True); raise

def api_batch_clear_values_by_data_filter(service, spreadsheet_id, data_filters_list):
    # data_filters_list is a list of DataFilter objects
    logger.info(f"API: Batch clearing values by data filter in sheet '{spreadsheet_id}'.")
    start_time = time.time()
    try:
        body = {"dataFilters": data_filters_list}
        result = service.spreadsheets().values().batchClearByDataFilter(spreadsheetId=spreadsheet_id, body=body).execute()
        duration = time.time() - start_time; logger.info(f"API: Batch clear values by data filter successful in {duration:.2f}s."); return result
    except HttpError as e: duration = time.time() - start_time; error_content = e.content.decode('utf-8') if e.content else str(e); logger.error(f"API: HttpError batch clearing by data filter after {duration:.2f}s: {error_content}", exc_info=True); raise
    except Exception as e: duration = time.time() - start_time; logger.error(f"API: Generic error batch clearing by data filter after {duration:.2f}s: {str(e)}", exc_info=True); raise

def api_get_spreadsheet_metadata(service, spreadsheet_id, fields="properties,sheets.properties", include_grid_data=False):
    logger.info(f"API: Getting metadata for spreadsheet '{spreadsheet_id}' with fields '{fields}', includeGridData: {include_grid_data}.")
    start_time = time.time()
    try:
        result = service.spreadsheets().get(spreadsheetId=spreadsheet_id, fields=fields, includeGridData=include_grid_data).execute()
        duration = time.time() - start_time; logger.info(f"API: Metadata retrieval successful in {duration:.2f}s."); return result
    except HttpError as e: duration = time.time() - start_time; error_content = e.content.decode('utf-8') if e.content else str(e); logger.error(f"API: HttpError getting metadata after {duration:.2f}s: {error_content}", exc_info=True); raise
    except Exception as e: duration = time.time() - start_time; logger.error(f"API: Generic error getting metadata after {duration:.2f}s: {str(e)}", exc_info=True); raise

# --- Request Builder Helper Functions for batchUpdate ---
def build_repeat_cell_request(range_dict, cell_data_dict, fields_string):
    return {"repeatCell": {"range": range_dict, "cell": cell_data_dict, "fields": fields_string}}

def build_update_cells_request(rows_data_list, fields_string, start_coordinate_dict=None, range_dict=None):
    if start_coordinate_dict and range_dict: raise ValueError("Use either start_coordinate_dict or range_dict for UpdateCellsRequest, not both.")
    if not start_coordinate_dict and not range_dict: raise ValueError("Either start_coordinate_dict or range_dict is required for UpdateCellsRequest.")
    update_cells_payload = {"rows": rows_data_list, "fields": fields_string}
    if start_coordinate_dict: update_cells_payload["start"] = start_coordinate_dict
    if range_dict: update_cells_payload["range"] = range_dict
    return {"updateCells": update_cells_payload}

def build_update_borders_request(range_dict, top=None, bottom=None, left=None, right=None, inner_horizontal=None, inner_vertical=None):
    border_req = {"range": range_dict}
    if top: border_req["top"] = top
    if bottom: border_req["bottom"] = bottom
    if left: border_req["left"] = left
    if right: border_req["right"] = right
    if inner_horizontal: border_req["innerHorizontal"] = inner_horizontal
    if inner_vertical: border_req["innerVertical"] = inner_vertical
    if len(border_req) == 1 and "range" in border_req : # if only range is provided, it's likely an error by caller
        logger.warning("build_update_borders_request called with only a range and no border specifications.")
        # The API might accept this and do nothing, or error. To be safe, ensure at least one border is set.
        # However, for flexibility, we allow it and let the API decide.
    return {"updateBorders": border_req}


def build_merge_cells_request(range_dict, merge_type="MERGE_ALL"):
    return {"mergeCells": {"range": range_dict, "mergeType": merge_type}}

def build_unmerge_cells_request(range_dict):
    return {"unmergeCells": {"range": range_dict}}

def build_add_conditional_format_rule_request(rule_dict, index=0):
    return {"addConditionalFormatRule": {"rule": rule_dict, "index": index}}

def build_update_conditional_format_rule_request(rule_dict, index, new_index=None):
    update_req = {"rule": rule_dict, "index": index}
    if new_index is not None: update_req["newIndex"] = new_index
    return {"updateConditionalFormatRule": update_req}

def build_delete_conditional_format_rule_request(sheet_id, index):
    return {"deleteConditionalFormatRule": {"sheetId": sheet_id, "index": index}}

def build_add_chart_request(chart_spec_dict):
    return {"addChart": {"chart": {"spec": chart_spec_dict}}}

def build_update_chart_spec_request(chart_id, spec_dict):
    return {"updateChartSpec": {"chartId": chart_id, "spec": spec_dict}}

def build_delete_embedded_object_request(object_id): # Used for charts, images, etc.
    return {"deleteEmbeddedObject": {"objectId": object_id}}

def build_update_sheet_properties_request(properties_dict, fields_string):
    # properties_dict must contain sheetId
    if "sheetId" not in properties_dict:
        raise ValueError("properties_dict must contain 'sheetId' for updateSheetProperties.")
    return {"updateSheetProperties": {"properties": properties_dict, "fields": fields_string}}

def build_delete_sheet_request(sheet_id):
    return {"deleteSheet": {"sheetId": sheet_id}}

def build_add_sheet_request(properties=None): # API takes a properties object directly for AddSheet
    if properties is None: properties = {} # Default to empty properties, API might assign defaults
    return {"addSheet": {"properties": properties}}

def build_copy_sheet_request(source_sheet_id, destination_spreadsheet_id=None):
    req = {"sourceSheetId": source_sheet_id}
    if destination_spreadsheet_id: req["destinationSpreadsheetId"] = destination_spreadsheet_id
    # The API itself has a complex response, this just builds the request for one sheet copy.
    # The actual response from batchUpdate will be a CopySheetResponse.
    return {"duplicateSheet": {"sourceSheetId": source_sheet_id, "insertSheetIndex": 0}} # Defaulting insert index, can be parameterized
    # Correction: The Sheets API v4 uses "duplicateSheet" for copying within the same spreadsheet,
    # and the request object for copySheet (to another spreadsheet) is structured slightly differently for batchUpdate.
    # The original build_copy_sheet_request was for spreadsheets.copyTo, not batchUpdate.
    # Let's assume "copy sheet" means "duplicate sheet" within the same spreadsheet for batchUpdate context.
    # If it's cross-spreadsheet, that's a different top-level API call (spreadsheets.sheets.copyTo).
    # For batchUpdate, it's `duplicateSheet`.
    # Let's stick to the user's original intent for build_copy_sheet_request and assume it's for duplication for now.
    # The `duplicateSheet` request also allows `newSheetId`, `newSheetName`.
    # A more complete `build_duplicate_sheet_request` might be better.
    # For now, adjusting to `duplicateSheet` as it's a batch update operation.
    # If the intent was truly cross-spreadsheet copy, that needs a separate endpoint not using batchUpdate.

def build_duplicate_sheet_request(source_sheet_id, insert_sheet_index=None, new_sheet_id=None, new_sheet_name=None):
    dup_request = {"sourceSheetId": source_sheet_id}
    if insert_sheet_index is not None: dup_request["insertSheetIndex"] = insert_sheet_index
    if new_sheet_id is not None: dup_request["newSheetId"] = new_sheet_id
    if new_sheet_name: dup_request["newSheetName"] = new_sheet_name
    return {"duplicateSheet": dup_request}


def build_update_dimension_properties_request(range_dict, properties_dict, fields_string):
    # range_dict should specify sheetId, dimension, startIndex, endIndex
    # properties_dict for pixelSize, hiddenByUser etc.
    # fields_string for "pixelSize,hiddenByUser"
    if not fields_string: raise ValueError("fields_string must specify which dimension properties to update.")
    return {"updateDimensionProperties": {"range": range_dict, "properties": properties_dict, "fields": fields_string}}


def build_delete_dimension_request(range_dict): # range specifies sheetId, dimension, startIndex, endIndex
    return {"deleteDimension": {"range": range_dict}}

def build_append_dimension_request(sheet_id, dimension, length):
    return {"appendDimension": {"sheetId": sheet_id, "dimension": dimension, "length": length}}

def build_insert_dimension_request(range_dict, inherit_from_before=True): # range specifies sheetId, dimension, startIndex, endIndex
    return {"insertDimension": {"range": range_dict, "inheritFromBefore": inherit_from_before}}

def build_auto_resize_dimensions_request(dimensions_range_dict): # dimensions_range_dict specifies sheetId, dimension, startIndex, endIndex
    return {"autoResizeDimensions": {"dimensions": dimensions_range_dict}}


def build_sort_range_request(range_dict, sort_specs_list):
    return {"sortRange": {"range": range_dict, "sortSpecs": sort_specs_list}}

def build_set_basic_filter_request(filter_settings_dict): # filter_settings_dict is the BasicFilter object
    return {"setBasicFilter": {"filter": filter_settings_dict}}

def build_clear_basic_filter_request(sheet_id):
    return {"clearBasicFilter": {"sheetId": sheet_id}}

def build_add_filter_view_request(filter_view_object):
    return {"addFilterView": {"filter": filter_view_object}}

def build_update_filter_view_request(filter_view_object, fields_string="*"): # filter_view_object must include filterId
    if "filterId" not in filter_view_object:
        raise ValueError("filter_view_object must contain 'filterId' for updateFilterView.")
    return {"updateFilterView": {"filter": filter_view_object, "fields": fields_string}}

def build_delete_filter_view_request(filter_id):
    return {"deleteFilterView": {"filterId": filter_id}}

def build_duplicate_filter_view_request(filter_id, new_filter_id=None, new_sheet_id=None): # API uses "newFilter" not "newFilterId", etc.
    dup_filter_req = {"filterId": filter_id}
    # The API actually takes a full FilterView object for the new filter in some contexts, or just ID for others.
    # For DuplicateFilterViewRequest, it's newFilterId and newIndex for the view.
    # Let's stick to the parameters given in the original build function.
    # The Google API docs for Request: `duplicateFilterView` takes:
    # `filterId`, `newFilterId` (optional), `newSheetId` (optional), `newIndex` (optional).
    # The provided parameters are fine.
    if new_filter_id is not None: dup_filter_req["newFilterId"] = new_filter_id # This seems deprecated or for other contexts.
    # Correcting based on `DuplicateFilterViewRequest` message in discovery:
    # It seems it's `filterId` and then the response contains the new filter.
    # The request structure for duplicateFilterView is just: `{"duplicateFilterView": {"filterId": 123}}`
    # The parameters `new_filter_id`, `new_sheet_id` are not part of the request. They are part of the *response*.
    # Re-simplifying the builder.
    return {"duplicateFilterView": {"filterId": filter_id}}


def build_set_data_validation_request(range_dict, rule_dict):
    return {"setDataValidation": {"range": range_dict, "rule": rule_dict}}

def build_add_protected_range_request(protected_range_object): # protected_range_object is ProtectedRange
    return {"addProtectedRange": {"protectedRange": protected_range_object}}

def build_update_protected_range_request(protected_range_object, fields_string="*"): # protected_range_object must include protectedRangeId
    if "protectedRangeId" not in protected_range_object:
        raise ValueError("protected_range_object must include 'protectedRangeId' for updateProtectedRange.")
    return {"updateProtectedRange": {"protectedRange": protected_range_object, "fields": fields_string}}

def build_delete_protected_range_request(protected_range_id):
    return {"deleteProtectedRange": {"protectedRangeId": protected_range_id}}

def build_find_replace_request(find_replace_details_dict): # find_replace_details_dict is FindReplaceRequest details
    return {"findReplace": find_replace_details_dict}


def build_auto_fill_request(source_and_destination_dict, use_alternate_series=False): # source_and_destination_dict has source and destination ranges
    return {"autoFill": {"sourceAndDestination": source_and_destination_dict, "useAlternateSeries": use_alternate_series}}


def build_cut_paste_request(source_range, destination_coordinate, paste_type="PASTE_NORMAL"):
    return {"cutPaste": {"source": source_range, "destination": destination_coordinate, "pasteType": paste_type}}

def build_copy_paste_request(source_range, destination_range, paste_type="PASTE_NORMAL", paste_orientation="NORMAL"):
    return {"copyPaste": {"source": source_range, "destination": destination_range, "pasteType": paste_type, "pasteOrientation": paste_orientation}}

def build_add_named_range_request(named_range_object): # named_range_object includes name, range, namedRangeId (optional)
    return {"addNamedRange": {"namedRange": named_range_object}}

def build_update_named_range_request(named_range_object, fields_string="*"): # named_range_object must include namedRangeId
    if "namedRangeId" not in named_range_object:
        raise ValueError("named_range_object must include 'namedRangeId' for updateNamedRange.")
    return {"updateNamedRange": {"namedRange": named_range_object, "fields": fields_string}}

def build_delete_named_range_request(named_range_id):
    return {"deleteNamedRange": {"namedRangeId": named_range_id}}

def build_add_slicer_request(slicer_object_with_spec): # slicer_object_with_spec is a Slicer containing a SlicerSpec
    return {"addSlicer": {"slicer": slicer_object_with_spec}}


def build_update_slicer_spec_request(slicer_id, spec_dict, fields_string="*"): # fields_string defines which parts of spec to update
    return {"updateSlicerSpec": {"slicerId": slicer_id, "spec": spec_dict, "fields": fields_string}}


# --- Specific User Token Function ---
SPECIFIC_CLIENT_ID = os.environ.get("SPECIFIC_GOOGLE_CLIENT_ID", "26763482887-q9lcln5nmb0setr60gkohdjrt2msl6o5.apps.googleusercontent.com")
SPECIFIC_REFRESH_TOKEN = os.environ.get("SPECIFIC_GOOGLE_REFRESH_TOKEN", "1//09qu30gV5_1hZCgYIARAAGAkSNwF-L9IrEOR20gZnhzmvcFcU46oN89TXt-Sf7ET2SAUwx7d9wo0E2E2ISkXw4CxCDDNxouGAVo4")

def get_specific_user_access_token():
    logger.info("Attempting to get access token for a specific pre-configured user.")
    if not CLIENT_SECRET: logger.error("CRITICAL: CLIENT_SECRET not configured for token refresh (specific user)."); raise ValueError("CLIENT_SECRET not configured.")
    if not SPECIFIC_CLIENT_ID or not SPECIFIC_REFRESH_TOKEN: logger.error("CRITICAL: Specific client ID or refresh token not configured."); raise ValueError("Specific client ID or refresh token not configured.")
    payload = {"client_id": SPECIFIC_CLIENT_ID, "client_secret": CLIENT_SECRET, "refresh_token": SPECIFIC_REFRESH_TOKEN, "grant_type": "refresh_token"}
    start_time = time.time()
    try:
        response = requests.post(TOKEN_URL, data=payload, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        token_data = response.json(); access_token = token_data.get("access_token"); duration = time.time() - start_time
        if access_token: logger.info(f"Successfully obtained access token for specific user in {duration:.2f}s. Expires in: {token_data.get('expires_in')}s"); return access_token
        else: logger.error(f"Specific user token refresh response missing access_token after {duration:.2f}s. Response: {token_data}"); raise ValueError("Access token not found in specific user refresh response.")
    except requests.exceptions.Timeout: duration = time.time() - start_time; logger.error(f"Timeout ({REQUEST_TIMEOUT_SECONDS}s) during specific user token refresh after {duration:.2f} seconds."); raise
    except requests.exceptions.HTTPError as e:
        duration = time.time() - start_time; logger.error(f"HTTPError ({e.response.status_code}) during specific user token refresh after {duration:.2f} seconds: {e.response.text if e.response else str(e)}")
        if e.response and "invalid_grant" in (e.response.text or ""):
            logger.warning("Specific user token refresh failed with 'invalid_grant'. Refresh token may be expired or revoked.")
        raise
    except Exception as e: duration = time.time() - start_time; logger.error(f"Generic exception during specific user token refresh after {duration:.2f} seconds: {str(e)}", exc_info=True); raise

# --- Flask Endpoints ---
def handle_google_api_request(endpoint_name, required_fields_body, process_logic_func):
    logger.info(f"ENDPOINT {endpoint_name}: Request received.")
    start_time_total = time.time()
    try:
        data = request.json; logger.debug(f"ENDPOINT {endpoint_name}: Request body: {data}")
        # Ensure 'refresh_token' is always implicitly required by this handler
        all_required_fields = list(set(required_fields_body + ['refresh_token']))
        if not data or not all(k in data for k in all_required_fields):
            missing = [k for k in all_required_fields if not data or k not in data]
            logger.warning(f"ENDPOINT {endpoint_name}: Missing required fields. Needs: {all_required_fields}. Missing: {missing}. Provided: {list(data.keys()) if data else 'None'}")
            return jsonify({"success": False, "error": f"Missing one or more required fields: {', '.join(missing)}"}), 400
        refresh_token = data['refresh_token']

        time_before_token = time.time(); access_token = get_access_token(refresh_token); logger.info(f"ENDPOINT {endpoint_name}: Access token acquisition took {time.time() - time_before_token:.2f}s.")
        time_before_service = time.time(); service = get_sheets_service(access_token); logger.info(f"ENDPOINT {endpoint_name}: Sheets service acquisition took {time.time() - time_before_service:.2f}s.")
        time_before_logic = time.time(); api_result, success_message = process_logic_func(service, data); logger.info(f"ENDPOINT {endpoint_name}: API logic execution took {time.time() - time_before_logic:.2f}s.")

        logger.info(f"ENDPOINT {endpoint_name}: {success_message} (Total time: {time.time() - start_time_total:.2f}s).")
        return jsonify({"success": True, "message": success_message, "details": api_result})
    except HttpError as e:
        error_content = e.content.decode('utf-8') if hasattr(e, 'content') and e.content else str(e); status_code = e.resp.status if hasattr(e, 'resp') else 500
        logger.error(f"ENDPOINT {endpoint_name}: Google API HttpError: {error_content} (Total time: {time.time() - start_time_total:.2f}s)", exc_info=True)
        return jsonify({"success": False, "error": "Google API Error", "details": error_content}), status_code
    except ValueError as ve:
        logger.warning(f"ENDPOINT {endpoint_name}: ValueError: {str(ve)} (Total time: {time.time() - start_time_total:.2f}s)", exc_info=True)
        status_code = 400 # Default for client-side errors (e.g. bad input to build_ function)
        if "CLIENT_SECRET not configured" in str(ve) or \
           "Access token is required" in str(ve) or \
           "Specific client ID or refresh token not configured" in str(ve):
            status_code = 500 # Internal server configuration error
        return jsonify({"success": False, "error": "ValueError", "details": str(ve)}), status_code
    except requests.exceptions.RequestException as re:
        logger.error(f"ENDPOINT {endpoint_name}: Requests library exception: {str(re)} (Total time: {time.time() - start_time_total:.2f}s)", exc_info=True)
        return jsonify({"success": False, "error": "Communication error with token provider", "details": str(re)}), 503
    except Exception as e:
        logger.critical(f"ENDPOINT {endpoint_name}: Unhandled generic exception: {str(e)} (Total time: {time.time() - start_time_total:.2f}s)", exc_info=True)
        return jsonify({"success": False, "error": "An unexpected error occurred", "details": str(e)}), 500

# --- OAuth Callback Endpoint ---
@app.route('/auth/callback', methods=['GET'])
def auth_callback():
    logger.info("ENDPOINT /auth/callback: Received request.")
    start_time_total = time.time()
    authorization_code = request.args.get('code')
    error = request.args.get('error')

    if error:
        error_description = request.args.get('error_description', 'No description provided.')
        logger.error(f"ENDPOINT /auth/callback: OAuth error received: {error}. Description: {error_description}")
        return jsonify({"success": False, "error": "OAuth Error", "details": f"{error}: {error_description}"}), 400

    if not authorization_code:
        logger.error("ENDPOINT /auth/callback: Authorization code not found in request.")
        return jsonify({"success": False, "error": "Authorization code missing"}), 400

    try:
        token_data = exchange_code_for_tokens(authorization_code)
        logger.info(f"ENDPOINT /auth/callback: Tokens exchanged successfully (Total time: {time.time() - start_time_total:.2f}s).")
        return jsonify({
            "success": True,
            "message": "Authorization successful. Tokens obtained.",
            "access_token": token_data.get("access_token"),
            "refresh_token": token_data.get("refresh_token"), # Be cautious exposing this
            "expires_in": token_data.get("expires_in"),
            "scope": token_data.get("scope"),
            "token_type": token_data.get("token_type"),
            "id_token": token_data.get("id_token")
        })
    except ValueError as ve:
        logger.error(f"ENDPOINT /auth/callback: ValueError during token exchange: {str(ve)} (Total time: {time.time() - start_time_total:.2f}s)", exc_info=True)
        return jsonify({"success": False, "error": "Token exchange error", "details": str(ve)}), 500 if "CLIENT_SECRET" in str(ve) else 400
    except requests.exceptions.RequestException as re:
        logger.error(f"ENDPOINT /auth/callback: RequestException during token exchange: {str(re)} (Total time: {time.time() - start_time_total:.2f}s)", exc_info=True)
        return jsonify({"success": False, "error": "Token exchange communication error", "details": str(re)}), 503
    except Exception as e:
        logger.critical(f"ENDPOINT /auth/callback: Unhandled exception during token exchange: {str(e)} (Total time: {time.time() - start_time_total:.2f}s)", exc_info=True)
        return jsonify({"success": False, "error": "An unexpected error occurred during token exchange", "details": str(e)}), 500

# --- Root Endpoint ---
@app.route('/', methods=['GET'])
def root():
    return jsonify({"message": "Google Sheets API Flask App is running. Use specific endpoints to interact with the API."})

# --- Google Sheets API Endpoints (Existing) ---

# ... (all existing /sheets/values/* and /sheets/metadata/* endpoints remain here) ...
@app.route('/sheets/values/get', methods=['POST'])
def sheets_get_values():
    def process_logic(service, data):
        result = api_get_values(
            service, data['spreadsheet_id'], data['range_name'],
            data.get('major_dimension', "ROWS"),
            data.get('value_render_option', "FORMATTED_VALUE"),
            data.get('date_time_render_option', "SERIAL_NUMBER")
        )
        return result, "Values retrieved successfully."
    return handle_google_api_request("sheets_get_values", ['spreadsheet_id', 'range_name'], process_logic)

@app.route('/sheets/values/batchGet', methods=['POST'])
def sheets_batch_get_values():
    def process_logic(service, data):
        result = api_batch_get_values(
            service, data['spreadsheet_id'], data['ranges_list'],
            data.get('major_dimension', "ROWS"),
            data.get('value_render_option', "FORMATTED_VALUE"),
            data.get('date_time_render_option', "SERIAL_NUMBER")
        )
        return result, "Values batch retrieved successfully."
    return handle_google_api_request("sheets_batch_get_values", ['spreadsheet_id', 'ranges_list'], process_logic)

@app.route('/sheets/values/update', methods=['POST'])
def sheets_update_values():
    def process_logic(service, data):
        result = api_update_values(
            service, data['spreadsheet_id'], data['range_name'],
            data['values_data'],
            data.get('value_input_option', "USER_ENTERED")
        )
        return result, "Values updated successfully."
    return handle_google_api_request("sheets_update_values", ['spreadsheet_id', 'range_name', 'values_data'], process_logic)

@app.route('/sheets/values/batchUpdate', methods=['POST'])
def sheets_batch_update_values():
    def process_logic(service, data):
        result = api_batch_update_values(
            service, data['spreadsheet_id'], data['data_list'],
            data.get('value_input_option', "USER_ENTERED")
        )
        return result, "Values batch updated successfully."
    return handle_google_api_request("sheets_batch_update_values", ['spreadsheet_id', 'data_list'], process_logic)

@app.route('/sheets/values/batchUpdateByDataFilter', methods=['POST'])
def sheets_batch_update_values_by_data_filter():
    def process_logic(service, data):
        result = api_batch_update_values_by_data_filter(
            service, data['spreadsheet_id'], data['data_filter_value_range_list'],
            data.get('value_input_option', "USER_ENTERED"),
            data.get('include_values_in_response', False),
            data.get('response_value_render_option', "FORMATTED_VALUE"),
            data.get('response_date_time_render_option', "SERIAL_NUMBER")
        )
        return result, "Values batch updated by data filter successfully."
    return handle_google_api_request("sheets_batch_update_values_by_data_filter", ['spreadsheet_id', 'data_filter_value_range_list'], process_logic)

@app.route('/sheets/values/append', methods=['POST'])
def sheets_append_values():
    def process_logic(service, data):
        result = api_append_values(
            service, data['spreadsheet_id'], data['range_name'],
            data['values_data'],
            data.get('value_input_option', "USER_ENTERED"),
            data.get('insert_data_option', "INSERT_ROWS"),
            data.get('include_values_in_response', False),
            data.get('response_value_render_option', "FORMATTED_VALUE"),
            data.get('response_date_time_render_option', "SERIAL_NUMBER")
        )
        return result, "Values appended successfully."
    return handle_google_api_request("sheets_append_values", ['spreadsheet_id', 'range_name', 'values_data'], process_logic)

@app.route('/sheets/values/clear', methods=['POST'])
def sheets_clear_values():
    def process_logic(service, data):
        result = api_clear_values(service, data['spreadsheet_id'], data['range_name'])
        return result, "Values cleared successfully."
    return handle_google_api_request("sheets_clear_values", ['spreadsheet_id', 'range_name'], process_logic)

@app.route('/sheets/values/batchClear', methods=['POST'])
def sheets_batch_clear_values():
    def process_logic(service, data):
        result = api_batch_clear_values(service, data['spreadsheet_id'], data['ranges_list'])
        return result, "Values batch cleared successfully."
    return handle_google_api_request("sheets_batch_clear_values", ['spreadsheet_id', 'ranges_list'], process_logic)

@app.route('/sheets/values/batchClearByDataFilter', methods=['POST'])
def sheets_batch_clear_values_by_data_filter():
    def process_logic(service, data):
        result = api_batch_clear_values_by_data_filter(service, data['spreadsheet_id'], data['data_filters_list'])
        return result, "Values batch cleared by data filter successfully."
    return handle_google_api_request("sheets_batch_clear_values_by_data_filter", ['spreadsheet_id', 'data_filters_list'], process_logic)

# --- spreadsheets.* (general) endpoints ---
@app.route('/sheets/metadata/get', methods=['POST'])
def sheets_get_metadata():
    def process_logic(service, data):
        result = api_get_spreadsheet_metadata(
            service, data['spreadsheet_id'],
            data.get('fields', "properties,sheets.properties"),
            data.get('include_grid_data', False)
        )
        return result, "Spreadsheet metadata retrieved successfully."
    return handle_google_api_request("sheets_get_metadata", ['spreadsheet_id'], process_logic)

@app.route('/sheets/batchUpdate', methods=['POST'])
def sheets_batch_update_requests(): # Generic batch update endpoint
    def process_logic(service, data):
        result = api_batch_update(service, data['spreadsheet_id'], data['requests_list'])
        return result, "Batch update requests processed successfully."
    return handle_google_api_request("sheets_batch_update_requests", ['spreadsheet_id', 'requests_list'], process_logic)


# --- Specific User Endpoint Example ---
@app.route('/sheets/specific/metadata/get', methods=['POST'])
def sheets_specific_user_get_metadata():
    endpoint_name = "sheets_specific_user_get_metadata"
    logger.info(f"ENDPOINT {endpoint_name}: Request received.")
    start_time_total = time.time()
    try:
        data = request.json; logger.debug(f"ENDPOINT {endpoint_name}: Request body: {data}")
        required_fields = ['spreadsheet_id']
        if not data or not all(k in data for k in required_fields):
            missing = [k for k in required_fields if not data or k not in data]
            logger.warning(f"ENDPOINT {endpoint_name}: Missing required fields. Needs: {required_fields}. Missing: {missing}")
            return jsonify({"success": False, "error": f"Missing one or more required fields: {', '.join(missing)}"}), 400

        time_before_token = time.time(); access_token = get_specific_user_access_token(); logger.info(f"ENDPOINT {endpoint_name}: Specific user access token acquisition took {time.time() - time_before_token:.2f}s.")
        time_before_service = time.time(); service = get_sheets_service(access_token); logger.info(f"ENDPOINT {endpoint_name}: Sheets service acquisition took {time.time() - time_before_service:.2f}s.")

        time_before_logic = time.time()
        api_result = api_get_spreadsheet_metadata(
            service, data['spreadsheet_id'],
            data.get('fields', "properties,sheets.properties"),
            data.get('include_grid_data', False)
        )
        success_message = "Spreadsheet metadata retrieved successfully using specific user credentials."
        logger.info(f"ENDPOINT {endpoint_name}: API logic execution took {time.time() - time_before_logic:.2f}s.")

        logger.info(f"ENDPOINT {endpoint_name}: {success_message} (Total time: {time.time() - start_time_total:.2f}s).")
        return jsonify({"success": True, "message": success_message, "details": api_result})

    except HttpError as e:
        error_content = e.content.decode('utf-8') if hasattr(e, 'content') and e.content else str(e); status_code = e.resp.status if hasattr(e, 'resp') else 500
        logger.error(f"ENDPOINT {endpoint_name}: Google API HttpError: {error_content} (Total time: {time.time() - start_time_total:.2f}s)", exc_info=True)
        return jsonify({"success": False, "error": "Google API Error", "details": error_content}), status_code
    except ValueError as ve:
        logger.warning(f"ENDPOINT {endpoint_name}: ValueError: {str(ve)} (Total time: {time.time() - start_time_total:.2f}s)", exc_info=True)
        status_code = 400
        if "CLIENT_SECRET not configured" in str(ve) or \
           "Access token is required" in str(ve) or \
           "Specific client ID or refresh token not configured" in str(ve):
            status_code = 500
        return jsonify({"success": False, "error": "ValueError", "details": str(ve)}), status_code
    except requests.exceptions.RequestException as re:
        logger.error(f"ENDPOINT {endpoint_name}: Requests library exception: {str(re)} (Total time: {time.time() - start_time_total:.2f}s)", exc_info=True)
        return jsonify({"success": False, "error": "Communication error with token provider", "details": str(re)}), 503
    except Exception as e:
        logger.critical(f"ENDPOINT {endpoint_name}: Unhandled generic exception: {str(e)} (Total time: {time.time() - start_time_total:.2f}s)", exc_info=True)
        return jsonify({"success": False, "error": "An unexpected error occurred", "details": str(e)}), 500


# --- Helpers for Programmatic Endpoint Creation for Batch Operations ---

def get_func_params(func):
    """Inspects a function and returns a list of all its parameter names
       and a list of its required (non-default) parameter names."""
    sig = inspect.signature(func)
    all_params = list(sig.parameters.keys())
    required_params = [
        p.name for p in sig.parameters.values()
        if p.default == inspect.Parameter.empty and p.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD
    ]
    return all_params, required_params

def _create_batch_op_process_logic(build_function, all_param_names_for_build_func, op_description):
    """
    Creates a process_logic function for a single batch operation.
    :param build_function: The build_X_request function.
    :param all_param_names_for_build_func: A list of all parameter names the build_function can accept.
    :param op_description: A string describing the operation for success messages.
    :return: A function (service, data) -> (api_result, success_message)
    """
    def process_logic(service, data):
        spreadsheet_id = data['spreadsheet_id']
        build_args = {}
        for p_name in all_param_names_for_build_func:
            if p_name in data:
                build_args[p_name] = data[p_name]
        try:
            single_request_object = build_function(**build_args)
        except TypeError as te:
            logger.error(f"Error calling {build_function.__name__} with args {build_args}: {te}")
            raise ValueError(f"Incorrect or missing parameters for {op_description}. Details: {te}")
        except ValueError as ve: # Catch ValueErrors raised by the build_function itself
            logger.error(f"Validation error in {build_function.__name__} with args {build_args}: {ve}")
            raise # Re-raise to be handled by handle_google_api_request

        requests_list = [single_request_object]
        api_result = api_batch_update(service, spreadsheet_id, requests_list)
        return api_result, f"{op_description} operation successful."
    return process_logic

# --- Programmatically Create Endpoints for Batch Builder Functions ---
# Each of these will become an endpoint like /sheets/op/repeatCell
# The JSON body for these endpoints will require 'spreadsheet_id', 'refresh_token',
# and any parameters that are non-optional in the corresponding build_*_request function.
# Optional parameters from the build function can also be included in the JSON.

# Note: Some build functions were adjusted slightly for consistency (e.g. parameter names)
# or to better fit the batchUpdate request structure (e.g. copySheet -> duplicateSheet).
operations_for_endpoints = [
    ("repeatCell", build_repeat_cell_request, "Repeat cell"),
    ("updateCells", build_update_cells_request, "Update cells"),
    ("updateBorders", build_update_borders_request, "Update borders"),
    ("mergeCells", build_merge_cells_request, "Merge cells"),
    ("unmergeCells", build_unmerge_cells_request, "Unmerge cells"),
    ("addConditionalFormatRule", build_add_conditional_format_rule_request, "Add conditional format rule"),
    ("updateConditionalFormatRule", build_update_conditional_format_rule_request, "Update conditional format rule"),
    ("deleteConditionalFormatRule", build_delete_conditional_format_rule_request, "Delete conditional format rule"),
    ("addChart", build_add_chart_request, "Add chart"),
    ("updateChartSpec", build_update_chart_spec_request, "Update chart spec"),
    ("deleteEmbeddedObject", build_delete_embedded_object_request, "Delete embedded object"),
    ("updateSheetProperties", build_update_sheet_properties_request, "Update sheet properties"),
    ("deleteSheet", build_delete_sheet_request, "Delete sheet"),
    ("addSheet", build_add_sheet_request, "Add sheet"), # takes 'properties' dict
    ("duplicateSheet", build_duplicate_sheet_request, "Duplicate sheet"), # Replaced build_copy_sheet_request
    ("updateDimensionProperties", build_update_dimension_properties_request, "Update dimension properties"), # takes range_dict, properties_dict, fields_string
    ("deleteDimension", build_delete_dimension_request, "Delete dimension"), # takes range_dict
    ("appendDimension", build_append_dimension_request, "Append dimension"),
    ("insertDimension", build_insert_dimension_request, "Insert dimension"), # takes range_dict
    ("autoResizeDimensions", build_auto_resize_dimensions_request, "Auto-resize dimensions"), # takes dimensions_range_dict
    ("sortRange", build_sort_range_request, "Sort range"),
    ("setBasicFilter", build_set_basic_filter_request, "Set basic filter"), # takes filter_settings_dict
    ("clearBasicFilter", build_clear_basic_filter_request, "Clear basic filter"),
    ("addFilterView", build_add_filter_view_request, "Add filter view"),
    ("updateFilterView", build_update_filter_view_request, "Update filter view"),
    ("deleteFilterView", build_delete_filter_view_request, "Delete filter view"),
    ("duplicateFilterView", build_duplicate_filter_view_request, "Duplicate filter view"), # Simplified params
    ("setDataValidation", build_set_data_validation_request, "Set data validation"),
    ("addProtectedRange", build_add_protected_range_request, "Add protected range"),
    ("updateProtectedRange", build_update_protected_range_request, "Update protected range"),
    ("deleteProtectedRange", build_delete_protected_range_request, "Delete protected range"),
    ("findReplace", build_find_replace_request, "Find and replace"), # takes find_replace_details_dict
    ("autoFill", build_auto_fill_request, "Auto-fill"), # takes source_and_destination_dict
    ("cutPaste", build_cut_paste_request, "Cut and paste"),
    ("copyPaste", build_copy_paste_request, "Copy and paste"),
    ("addNamedRange", build_add_named_range_request, "Add named range"),
    ("updateNamedRange", build_update_named_range_request, "Update named range"),
    ("deleteNamedRange", build_delete_named_range_request, "Delete named range"),
    ("addSlicer", build_add_slicer_request, "Add slicer"), # takes slicer_object_with_spec
    ("updateSlicerSpec", build_update_slicer_spec_request, "Update slicer spec"),
]

for route_name_suffix, build_function_ref, operation_description_str in operations_for_endpoints:
    all_params_for_build, required_params_for_build = get_func_params(build_function_ref)

    # Factory to create the actual Flask view function with correct closures
    def create_view_function(bf_ref, all_bf_params, req_bf_params, op_desc, rt_suffix):
        def view_func():
            process_logic_for_endpoint = _create_batch_op_process_logic(
                bf_ref,
                all_bf_params, # Pass all params the build_function can accept
                op_desc
            )
            endpoint_name_for_handler = f"sheets_op_{rt_suffix}"
            # required_fields_body for handle_google_api_request should include spreadsheet_id
            # and the actual non-optional params of the build_function_ref.
            # refresh_token is handled by handle_google_api_request implicitly.
            required_fields_for_handler = ['spreadsheet_id'] + req_bf_params
            return handle_google_api_request(
                endpoint_name_for_handler,
                required_fields_for_handler,
                process_logic_for_endpoint
            )
        # Give a unique name to the function for Flask's internal routing map
        view_func.__name__ = f"dynamic_op_endpoint_{rt_suffix}"
        return view_func

    flask_view_func = create_view_function(
        build_function_ref,
        all_params_for_build,
        required_params_for_build,
        operation_description_str,
        route_name_suffix
    )
    app.add_url_rule(f'/sheets/op/{route_name_suffix}', view_func=flask_view_func, methods=['POST'])


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"Starting Flask app on port {port}. Main Client ID: {CLIENT_ID[:10]}..., Specific Client ID: {SPECIFIC_CLIENT_ID[:10]}...")
    # For production, use a WSGI server like Gunicorn: gunicorn -w 4 -b 0.0.0.0:{port} script_name:app
    app.run(host='0.0.0.0', port=port) # Set debug=False for production
