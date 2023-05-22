from shareplum import Site
from shareplum import Office365
from shareplum.site import Version
from configparser import ConfigParser
import os
import logging
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File 

class commons_sharepoint:

    def download_file_from_sharepoint(file_name, target_folder=None, local_folder=None, sectionName=None, config = None):

        import configparser
        filename = os.path.expanduser('~') + '' + '/.sharecare/gusto_credentials.cfg'
        parser = configparser.ConfigParser()
        parser.read(filename)

        if(sectionName == None):
            sectionName = "sharepoint"
        if(config == None):
            config = {}
            if parser.has_section(sectionName):
                items = parser.items(sectionName)
                for item in items:
                    config[item[0]] = item[1]

        #logging.info(f"logging to : {config['homepage']} using login name : {config['username']}")

        ctx_auth = AuthenticationContext(config["homepage"])
        if ctx_auth.acquire_token_for_user(config['username'], config["password"]):
            ctx = ClientContext(config["homepage"], ctx_auth)
            web = ctx.web
            ctx.load(web)
            ctx.execute_query()
            #logging.info(f"Connected to : {config['homepage']}")
            
        if(target_folder == None):
            target_folder = config['targetfolder']
        file_download_path = f"{target_folder}/{file_name}"
        response = File.open_binary(ctx, file_download_path)

        if(local_folder == None):
            local_file_path=file_name
        else:
            local_file_path = f"{local_folder}/{file_name}"
        response.raise_for_status()
        with open(local_file_path, "wb") as local_file:
            local_file.write(response.content)
            
            #logging.info(f"downloaded {file_name} successfully to : {local_file_path}")

    def download_multiple_files_from_sharepoint(target_folder=None, local_folder=None, sectionName=None, config = None):

        import configparser
        filename = '/Users/giriprakash/Desktop/files_downloader/sharecare/gusto_credentials.cfg'
        parser = configparser.ConfigParser()
        parser.read(filename)

        if(sectionName == None):
            sectionName = "sharepoint"
        if(config == None):
            config = {}
            if parser.has_section(sectionName):
                items = parser.items(sectionName)
                for item in items:
                    config[item[0]] = item[1]

            #logging.info(f"logging to : {config['homepage']} using login name : {config['username']}")

        ctx_auth = AuthenticationContext(config["homepage"])
        if ctx_auth.acquire_token_for_user(config['username'], config["password"]):
            ctx = ClientContext(config["homepage"], ctx_auth)
            web = ctx.web
            ctx.load(web)
            ctx.execute_query()
            #logging.info(f"Connected to : {config['homepage']}")

        if(target_folder == None):
            target_folder = config['targetfolder']

        root_folder = ctx.web.get_folder_by_server_relative_path(target_folder)
        files = root_folder.get_files(True).execute_query()

        for f in files:
            try: 
                if 'LN_Mapping_Specification' in str(f.properties['ServerRelativeUrl']):
                    file_url = open(f"{local_folder}/files_url.txt", "a")
                    file_url.write(f"{f.properties['ServerRelativeUrl']},")
                    download_path = os.path.join(local_folder, os.path.basename(f.properties['ServerRelativeUrl']))
                    with open(download_path, "wb") as local_file:
                        file = ctx.web.get_file_by_server_relative_url(f.properties['ServerRelativeUrl']).download(local_file).execute_query()

                    #logging.info(f"downloaded {f.name} successfully to : {download_path}")
                    file_url.close()
                else:
                    print("no mapping tab")
            except:
                print("not able to download")

    def download_multiple_files_from_sharepoint2(target_folder=None, local_folder=None, sectionName=None, config=None):
        import configparser
        filename = os.path.expanduser('~') + '' + '/.sharecare/gusto_credentials.cfg'
        parser = configparser.ConfigParser()
        parser.read(filename)

        try:
            if sectionName is None:
                sectionName = "sharepoint"
            if config is None:
                config = {}
                if parser.has_section(sectionName):
                    items = parser.items(sectionName)
                    for item in items:
                        config[item[0]] = item[1]

            logging.info(f"logging to : {config['homepage']} using login name : {config['username']}")

            ctx_auth = AuthenticationContext(config["homepage"])
            if ctx_auth.acquire_token_for_user(config['username'], config["password"]):
                ctx = ClientContext(config["homepage"], ctx_auth)
                web = ctx.web
                ctx.load(web)
                ctx.execute_query()
            logging.info(f"Connected to : {config['homepage']}")

            if target_folder is None:
                target_folder = config['targetfolder']

            root_folder = ctx.web.get_folder_by_server_relative_path(target_folder)
            query = root_folder.files.filter("startswith(ServerRelativeUrl, '/{}/') and substringof('LN_Mapping_Specification', ServerRelativeUrl)".format('/'.join(client.lower() for client in clients)))
            files = ctx.load(query).execute_query()

            for f in files:
                try:
                    download_path = os.path.join(local_folder, os.path.basename(f.properties['ServerRelativeUrl']))
                    with open(download_path, "wb") as local_file:
                        f.download(local_file)
                    logging.info(f"downloaded {f.name} successfully to : {download_path}")
                except Exception as e:
                    logging.error(f"{f.name} file raised an error", exc_info=True)
                    raise e

        except Exception as e:
            logging.error(f"error in download_multiple_files_from_sharepoint: {str(e)}", exc_info=True)
            raise e
