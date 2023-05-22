from configparser import ConfigParser
import os
import logging
import vertica_python

def db_config(section,domain=None):
    """Get the configuration values from config file

    Arguments:
        section -- Name of the section in config file

    Raises:
        Exception: raises exception when section is not found in config file

    Returns:
        dictionary -- returns a dictionary of configuration
    """
    if domain == 'sharecare_etl':
        filename = '/Users/giriprakash/Desktop/files_downloader/sharecare/gusto_credentials.cfg'
    else:    
        filename = '/Users/giriprakash/Desktop/files_downloader/sharecare/gusto_credentials.cfg'
    # create parser and read ini configuration file
    parser = ConfigParser(interpolation=None)
    parser.read(filename)
    # get section
    db = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            db[item[0]] = item[1]
    else:
        raise Exception(f"{section} not found in the {filename} file")
    return db

def get_config_value(section, configName):
    """
    Returns configuration value from given section in the config file
    
    Arguments:
        section {string} -- Name of the section in config file
        configName {string} -- property or configuration name under the section
    
    Returns:
        string -- Returns value of the configuration if exists
    """
    import configparser
    import os
    filename = '/Users/giriprakash/Desktop/files_downloader/sharecare/gusto_credentials.cfg'
    config = configparser.ConfigParser()
    config.read(filename)
    configValue = ''
    try:
        configValue =  config.get(section, configName)
        logging.info(f"Configuration read for section : {section}")
    except Exception as exception:
        logging.error(f"Exception occured: {exception}", exc_info=True)
        logging.error(f'{section}, {configName} not found in the {filename} file')
        raise exception
    return configValue


def get_merge_sql(schema, table, sec_table=None, connection=None):
    """[summary]
            Aim: 
                Takes connection, schema and table name and return dynamic merge statement 

            Arguments:
                connection {[object]} -- [Takes the connection object]
                schema {[str]} -- [schema name]
                table {[str]} -- [Table name]
                sec_table {[str]} -- [seconday table that need to be merged with stage table]

            Returns:
                merge [str] -- [Gives the merge statement for the schema and table name provided as input]
            """
    import pandas as pd

    if not connection:
        # connect to vertica
        vertica_db = db_config(section="vertica", domain='sharecare_etl')
        connection = vertica_python.connect(**vertica_db)
        
    def get_table_lists(schema, table):
        all_sql = f"""SELECT column_name FROM v_catalog.columns WHERE  table_schema='{schema}' AND lower(table_name)='{table.lower()}';"""
        all_df = pd.read_sql(sql=all_sql, con=connection)
        all_list = list(all_df["column_name"])

        pk_sql = f"select column_name FROM v_catalog.primary_keys WHERE  table_schema = '{schema}' AND lower(table_name) = '{table.lower()}' order by table_schema, table_name, ordinal_position;"
        pk_df = pd.read_sql(sql=pk_sql, con=connection)
        pk_list = list(pk_df["column_name"])

        ulist = list(set(all_list).symmetric_difference(set(pk_list)))

        return all_list, pk_list, ulist

    # * * Main join condition for merge statement

    def get_merge_lists(pk_list, ulist, all_list):
        flist = []
        for index, elem in enumerate(pk_list):
            cond = "SRC." + str(elem) + "=" + "TGT." + str(elem)
            if index == 0 or index == len(pk_list) - 1:
                if index == 0:

                    if len(pk_list) == 1:
                        flist.append(cond)
                    else:
                        flist.append(cond + " AND")
                else:
                    flist.append(cond)
            else:
                flist.append(cond + " AND")

        main_list = " ".join(flist)

        # * * update condition string only on non primary key columns

        update_list = []
        for index, elem in enumerate(ulist):
            cond = str(elem) + "=" + "SRC." + str(elem)
            if index == 0 or index == len(ulist) - 1:
                if index == 0:
                    update_list.append(cond)
                else:
                    update_list.append(cond)
            else:
                update_list.append(cond)

        update_list = ", ".join(update_list)

        # * * insert condition String

        insert_list = []
        for index, elem in enumerate(all_list):
            cond = "SRC." + str(elem)
            if index == 0 or index == len(all_list) - 1:
                if index == 0:
                    insert_list.append(cond)
                else:
                    insert_list.append(cond)
            else:
                insert_list.append(cond)

        insert_list = " , ".join(insert_list)

        return main_list, update_list, insert_list

    # * * Generating final merge statements with main condition, update condition and insert condition

    if sec_table is not None:
        all_list, pk_list, _ = get_table_lists(schema=schema, table=table)
        all_list_sec, pk_list_sec, ulist_sec = get_table_lists(
            schema=schema, table=sec_table
        )
        # getting the common columns for initial table and given seconday table

        common = [col for col in all_list if col in all_list_sec]
        ulist_sec = common + pk_list
        main_list_sec, update_list_sec, insert_list_sec = get_merge_lists(
            pk_list=pk_list, ulist=common, all_list=common
        )

        merge_sql = f"""MERGE INTO {schema}.{sec_table.lower()} TGT USING {schema}.{table.lower()}_stage SRC ON {main_list_sec} WHEN MATCHED THEN UPDATE SET {update_list_sec} WHEN NOT MATCHED THEN INSERT VALUES ({insert_list_sec});"""

    else:
        all_list, pk_list, ulist = get_table_lists(schema=schema, table=table)
        main_list, update_list, insert_list = get_merge_lists(
            pk_list=pk_list, ulist=ulist, all_list=all_list
        )

        merge_sql = f"""MERGE INTO {schema}.{table.lower()} TGT USING {schema}.{table.lower()}_stage SRC ON {main_list} WHEN MATCHED THEN UPDATE SET {update_list} WHEN NOT MATCHED THEN INSERT VALUES ({insert_list});"""
    connection.close()
    return merge_sql


def write_to_db(
    str_data, schema, table, connection=None, sep=None, cols=None, **kwargs
):
    """Write | separated data to vertica table

    Arguments:
        str_data  -- Input data
        schema  -- Schema of the table
        table  -- Vertica table to save the data

    Keyword Arguments:
        connection  -- vertica connection details (default: {None})

    Raises:
        exception: raise exception if data could not be written
    """

    try:
        if connection == None:
            connection = vertica_python.connect(**kwargs)
        if sep == None:
            sep = "|"
        cur = connection.cursor()
        if cols == None:
            cur.copy(
                f"COPY {schema}.{table} from stdin DELIMITER '{sep}' ENCLOSED BY '\"'  ABORT ON ERROR;",
                str_data,
            )
        else:
            cur.copy(
                f"COPY {schema}.{table} ({cols}) from stdin DELIMITER '{sep}' ENCLOSED BY '\"' ABORT ON ERROR;",
                str_data,
            )
        logging.info(f"Inserted data into {schema}.{table}")
    except Exception as exception:
        logging.error(f"Data failed to insert...Exception raised {exception}", exc_info=True)
        raise exception