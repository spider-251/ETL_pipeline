import os
import pandas as pd
from sqlalchemy import create_engine
import logging
from lib.commons_db import db_config
from pathlib import Path
from lib.commons_sharepoint import commons_sharepoint
import glob
import argparse
import hashlib
# from lib.commons_load_stats import commons_load_stats
# from lib.commons_load_stats_base import commons_load_stats_base

def download_file_to_local(category=None,client=None,provider=None):
    """
    This function downloads the files from target folder based on category, client and provider
    
    Parameters:
    category (string) (optional): pass category to download all files from particular category
    client (string) (optional): pass client to download all files from particular client
    provider (string) (optional): pass provider to download all files from particular proivder

    Pass None to download all files from the target folder and sub directories.
    
    Returns:
    None
    """
    p = Path(tmp_location)
    if not p.exists():
        logging.info("creating directory for given file_location")
        p.mkdir(parents=True)
    #logging.info(f"downloading filename to local")
    commons_sharepoint.download_multiple_files_from_sharepoint(target_folder=target_folder, 
                                                                local_folder=tmp_location,
                                                                sectionName='sharepoint')
    #logging.info(f"filename downloaded to local")
    return None

def db_connection():
    """
    This function initiates the connection with mysql
    """
    mysql_config = db_config(section="dataops_mysql")
    connection = create_engine('mysql+pymysql://'+str(list(mysql_config.values())[1])+':'+
                                                str(list(mysql_config.values())[2])+'@'+
                                                str(list(mysql_config.values())[0])+'/'+
                                                str(list(mysql_config.values())[3]))
    return connection

def get_file_hash(file):
    BLOCK_SIZE = 65536
    file_hash = hashlib.sha256()
    with open(file, 'rb') as f:
        fb = f.read(BLOCK_SIZE)
        while len(fb) > 0:
            file_hash.update(fb)
            fb = f.read(BLOCK_SIZE)
    return file_hash.hexdigest()

def parsing_filename():
    """
    This function takes a files_url.txt from tmp_location consists of origin location of download files and
    use it to parse the category, client, provider and filename.
    
    Parameters:
    tmp_location (string): the local location of the folder of excel files
    
    Returns:
    list: a list containing the file name, category, client and provider parsed from the file location
    """
    f = open(f"{tmp_location}/files_url.txt", 'r').read()
    items = f.split(',')
    items = filter(bool,items)
    spec_details = {}
    for item in items:
        try:
            category = item.split('/')[10]
            client = item.split('/')[9]
            provider = item.split('/')[11]
            filename = item.split('/')[-1]
            spec_details[filename] = {"category":category,"client":client,"provider":provider}
        except Exception as e:
            raise e
    return spec_details

def check_filename_exists(connection, filename):
    """This function checks if the filename exists in the mysql table in the file_name column"""
    query = "SELECT EXISTS (SELECT 1 FROM wyse_kpi.mapping_specifications WHERE file_name = '{}')".format(filename)
    result = connection.execute(query).fetchone() # check if filename exists
    return result[0]

def check_filehash_exists(connection, filename, file_hash):
    """This function checks if the file hash exists in the mysql table"""
    query = "SELECT EXISTS (SELECT 1 FROM wyse_kpi.mapping_specifications WHERE file_name = '{}' AND file_hash = '{}')".format(filename,file_hash)
    result = connection.execute(query).fetchone() # check if filehash exists
    return result[0]

def update_existing_file(connection, file_name):
    """This function updates the existing file values in the mysql table"""
    query = "UPDATE wyse_kpi.mapping_specifications SET version=version+1, modified_date=NOW() WHERE file_name = %s"
    connection.execute(query, file_name)

def update_new_file(connection, file_name):
    """This function updates the new file values in the mysql table"""
    query = "UPDATE wyse_kpi.mapping_specifications SET version=1, created_date=NOW() WHERE file_name = %s"
    connection.execute(query, file_name)

def update_db(filename, connection, file_hash, sheet_name = 'Mapping'):
    """
    This function takes a folder of excel files from tmp_location and updates the advocacy_mapping_specifications table in the database.
    
    Parameters:
    tmp_location (string): the location of the folder of excel files
    file_details (list): a list containing the file name, version, and created date of any files that are present in the table with the same file name but a different hash
    sheet_name (string): specific sheetname to extract values
    
    Returns:
    None
    """
    spec_details = parsing_filename()
        
    # Read the excel file
    df = pd.read_excel(filename,sheet_name=sheet_name, header=0, engine='openpyxl')

    #get category, client and provider details of particular file
    detail = spec_details[filename]

    data_renamed = df.rename(columns={'Raw Field Name':'raw_field_name',
                                            'Format/Values':'format_values', 
                                            'Raw Field Example Value/Format':'format_values',
                                            'DM Field':'dm_field', 
                                            'Data Model Field':'dm_field',
                                            'Field':'dm_field',
                                            'Description':'description', 
                                            'Data Type':'data_type'})

    final_data = data_renamed.assign(category_name = detail['category'], 
                                    client_name = detail['client'], 
                                    provider_name = detail['provider'], 
                                    file_name=filename, 
                                    file_hash=file_hash)

    final_data.to_sql(name="mapping_specifications", con=connection, index=False, if_exists='append')
    return None

def main(connection):
    path = tmp_location
    os.chdir(path)
    files = glob.glob('*.{}'.format('xlsx'))
    for filename in files:

        file_hash = get_file_hash(filename) # calculate file hash
        filename_exists = check_filename_exists(connection, filename) # check if filename exists
        filehash_exists = check_filehash_exists(connection,filename, file_hash) # check if file hash exists

        if not filename_exists and not filehash_exists: # if filename and filehash doesn't exist 
            print('no files found updating')
            update_db(filename, connection, file_hash)
            update_new_file(connection, filename) # update new file values

        elif filename_exists and not filehash_exists: # if filename exists and filehash doesn't exist
            print('file hash not found updating')
            created_date = connection.execute("SELECT created_date FROM wyse_kpi.mapping_specifications WHERE file_name = %s", filename).fetchone() # get the created date
            connection.execute("DELETE FROM wyse_kpi.mapping_specifications WHERE file_name = %s", filename) # delete values with same filename
            print('updating values')
            update_db(filename, connection, file_hash)
            print('updating version and modified dates')
            update_existing_file(connection, filename) # update new file values
            print('updating the created_date')
            connection.execute("UPDATE wyse_kpi.mapping_specifications SET created_date='{}' WHERE file_name = '{}'".format(created_date[0], filename)) # update created date
        
        else: # if filename and filehash exists
            print("table up-to-date")
            pass # do nothing

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='download file from sharepoint',
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                    allow_abbrev=False)
    
    parser.add_argument('--category', '-c',
                        type=str,
                        dest="category",
                        required=False,
                        choices= {'eligibility', 'medclaims', 'rxclaims', 'advocacy','authorizations','visionclaims','dentalclaims','dentalaccumulator'},
                        help='category to process')

    parser.add_argument('--client', '-cl',
                        type=str,
                        dest="client",
                        required=False,
                        choices= {'koch', 'sharecare', 'kohler'},
                        help='client to process')

    parser.add_argument('--provider', '-p',
                        type=str,
                        dest="provider",
                        required=False,
                        default='default',
                        choices= {'default'},
                        help='client to process')

    args, unknown = parser.parse_known_args()
    category = args.category
    client = args.client
    provider = args.provider
    
    sharepoint_config = db_config(section="sharepoint")
    tmp_location = sharepoint_config['tmp_location']
    
    try:
        # etl_mode = db_config("env_mode")["etl_mode"]
        # if etl_mode == 'dev':
        #     load_stats = commons_load_stats_base()
        # else:
        #     load_stats = commons_load_stats()
        # load_stats.insert_job_start_log(job_type="Download_and_extract_file_to_mysql", 
        #                                 status="running", 
        #                                 source_system_name="sharepoint", 
        #                                 target_system_name="MySql", 
        #                                 source_system_location="sharepoint", 
        #                                 target_system_location="Mysql")


        if category and client and provider:
            target_folder = sharepoint_config['targetfolder'] + f'/specifications/{client}/{category}/{provider}'
        elif category and client:
            target_folder = sharepoint_config['targetfolder'] + f'/specifications/{client}/{category}'
        elif category:
            target_folder = sharepoint_config['targetfolder'] + f'/specifications/{client}'
        else:
            target_folder = sharepoint_config['targetfolder']

        download_file_to_local()
        connection = db_connection()
        main(connection)
        connection.dispose()
        #shutil.rmtree(tmp_location)
        
        #load_stats.update_job_complete_log()

    except Exception as ex:
        #logging.error(f"error in excel_mysql_extractor: {str(ex)}", exc_info=True)
        #load_stats.update_job_failed_log(str(ex)[:200])
        raise ex