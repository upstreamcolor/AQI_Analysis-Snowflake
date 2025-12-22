import requests
import json
from datetime import datetime
from snowflake.snowpark import Session
from dateutil.parser import parse
import sys
import pytz
from dotenv import load_dotenv
load_dotenv()
import os
import logging

# initiate logging at info level
logging.basicConfig(stream = sys.stdout, level = logging.INFO, format = '%(levelname)s - %(message)s')

# authenticating to the Snowflake account
def snowpark_basic_auth() -> Session:
    connection_parameters = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT")
        ,"user": os.getenv("SNOWFLAKE_USER")
        ,"password": os.getenv("SNOWFLAKE_PASSWORD")
        ,"role": os.getenv("SNOWFLAKE_ROLE")
        ,"warehouse": os.getenv("SNOWFLAKE_WAREHOUSE")
        ,"database": os.getenv("SNOWFLAKE_DATABASE")
        ,"schema": os.getenv("SNOWFLAKE_SCHEMA")
    }
    # creating snowflake session object
    return Session.builder.configs(connection_parameters).create()

def get_air_quality_data(api_key, limit):
    api_url = os.getenv("API_URL")
    
    # parameters for the API request
    params = {
        'api-key': api_key,
        'format': 'json',
        'limit': limit
    }

    # headers for the API request
    headers = {
        'accept': 'application/json'
    }

    try:
        # Make the GET request
        response = requests.get(api_url, params = params, headers = headers)

        logging.info('Response received from the API')
        
        # check if the request was successful (status code 200)
        if response.status_code == 200:

            logging.info('Response received succesfully')
            
            # parse the JSON data from the response
            json_data = response.json()
            
            # checking whether `records` is present or not
            records = json_data.get("records")

            if not records or len(records) == 0:
                logging.error("API response contains no records.")
                sys.exit(1)

            # get the first occurence of `last_update` from records [{}]
            last_update_str = json_data.get("records", [{}])[0].get("last_update")
            if not last_update_str:
                logging.error("No last_update found in API response.")
                sys.exit(1)

            last_update = parse(last_update_str)

            # create the file name
            file_name = f'air_quality_data_{last_update.strftime("%Y_%m_%d_%H_%M_%S")}.json'

            # get today's date to create a directory with the same
            today_string = last_update.strftime('%Y_%m_%d')
            stg_location = '@dev_db.stage_sch.raw_stg/india/'+ today_string +'/'

            # authenticate to snowflake
            sf_session = snowpark_basic_auth()
            
            logging.info('Snowflake login successful...')
            
            # list query to check existence of file to be uploaded in stage
            list_query = f'list {stg_location}{file_name}.gz'
            
            # check for the existence of the file in stage
            logging.info(f'List query to fetch the stage file to check its existence = {list_query}')
            result_list = sf_session.sql(list_query).collect()

            if result_list:
                logging.info(f'File {file_name} already exists in stage {stg_location}, skipping upload.')
            else:
                logging.info('Writing the JSON file into local location before it is moved to snowflake stage')

                # save the JSON data to a file
                with open(file_name, 'w') as json_file:
                    json.dump(json_data, json_file, indent = 2)
                
                logging.info(f'File written to local disk with name: {file_name}')

                # place the file from local to snowflake stage
                logging.info(f'Placing the file, {file_name} in stage location, {stg_location}')
                sf_session.file.put(file_name, stg_location)

                logging.info(f'JSON File placed successfully in stage location, {stg_location} in snowflake')
            
                logging.info('The job completed successfully...')
            
            # return the retrieved data
            return json_data

        else:
            # print an error message if the request was unsuccessful
            logging.error(f"{response.status_code} - {response.text}")
            sys.exit(1)
            #return None

    except Exception as e:
        # handle exceptions, if any
        logging.error(f'{e}')
        sys.exit(1)
        
    return None

api_key = os.getenv("API_KEY")
limit_value = int(os.getenv("API_LIMIT", "4000"))
air_quality_data = get_air_quality_data(api_key, limit_value)