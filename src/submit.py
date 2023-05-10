
import images.omero as omr
import images.climb as climb
import images.jaxlims as jxl
import utils
import logging
import os
import socket
import sys
import datetime
from collections import defaultdict
from errno import errorcode
from getpass import getpass
from logging.handlers import RotatingFileHandler
from typing import Any
import mysql.connector
import paramiko
import requests
from requests import exceptions
from urllib3.connection import HTTPConnection

"""Setup logger"""

logger = logging.getLogger(__name__)
FORMAT = "[%(asctime)s->%(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT, filemode="w", level=logging.DEBUG, force=True)
logging_dest = os.path.join(utils.get_project_root(), "logs")
date = datetime.date.today().strftime("%B-%d-%Y")
logging_filename = logging_dest + "/" + f'{date}.log'
handler = RotatingFileHandler(logging_filename, maxBytes=10000000000, backupCount=10)
handler.setFormatter(logging.Formatter(FORMAT))
logger.addHandler(handler)

try:
    os.mkdir(logging_dest)

except OSError as e:
    print(e)



def main():
    """Set a higher timeout tolerance"""
    HTTPConnection.default_socket_options = (HTTPConnection.default_socket_options + [
        (socket.SOL_SOCKET, socket.SO_SNDBUF, 1000000),
        (socket.SOL_SOCKET, socket.SO_RCVBUF, 1000000)
    ])

    if sys.argv[1] == "-u":

        print("Where do you want to download your images from?\n")
        downloadSource = input("Enter images source:")

        if downloadSource == "Omero":

            targetPath = os.path.join(utils.get_project_root(), "KOMP_images", "Omeros")
            logger.debug(f"Target path is {targetPath}")
            try:
                os.mkdir(targetPath)

            except FileExistsError as e:
                logger.warning("File/folder exists")

            server = "rslims.jax.org"
            username = "dba"
            password = "rsdba"
            database = "rslims"

            conn = omr.db_init(server=server, username=username, password=password, database=database)
            sql = utils.stmt.format("%omeroweb%")

            print("How would you like to select thhe images?")
            query_filters = {#"DateDue": input("Date:"),
                            "ProcedureStatus": input("Status:"),
                                "OrganismID": input("Animal ID:"),
                                "Output.ExternalID": input("IMPC Code:")}

            """Form the sql queries based on user input"""
            for key, val in query_filters.items():
                if not query_filters[key]:
                    continue
                if key == "DateDue":
                    val = val.replace("-", "")
                    sql = sql + f" AND DateDue >= {val}"
                else:
                    filter_clause = f" AND {key} LIKE '%{val}%'"
                    sql = sql + filter_clause

            sql = sql + ";"
            #print(sql)
            print()
            print("Please enter your username and password to log in")
            username = input("Username: ")
            password = getpass("Password: ")
            omr.download_from_omero(username=username, password=password, download_to=targetPath, conn=conn, sql=sql)
            conn.close()

        '''Download images in JaxLims'''
        if downloadSource == "JaxLims":
            server = "rslims.jax.org"
            username = "dba"
            password = "rsdba"
            database = "rslims"

            conn = jxl.db_init(server=server, username=username, password=password, database=database)

            print("How would you like to select?")
            query_filters = {"DateDue": input("Date:"),
                                "OrganismID": input("Animal ID:"),
                                "ExternalID": input("Parameter Code:")}


            '''Remove duplciate records'''

            sql = utils.stmt("%phenotype%")
            for key, val in query_filters.items():
                if not query_filters[key]:
                    continue
                if key == "DateDue":
                    val = val.replace("-", "")
                    sql = sql + f" AND DateDue >= {val}"
                else:
                    filter_clause = f" AND {key} LIKE '%{val}%'"
                    sql = sql + filter_clause

            sql = sql + ";"

            # List stores additional condition on sql query
            targetPath = os.path.join(utils.get_project_root(), "KOMP_images", "JaxLims")
            fileLocationMap = jxl.buildFileMap(conn=conn, sql=sql, target=targetPath)
            # srcPath = "//bht2stor.jax.org/" #If you are on windows
            srcPath = "/Volumes/"  # If you are on mac/linux
            jxl.download_from_drive(fileLocationMap, source=srcPath, target=targetPath)

        if downloadSource == "Climb":
            # If you are on windows
            # srcPath = "//bht2stor.jax.org/"

            srcPath = "/Volumes/"
            targetPath = os.path.join(utils.get_project_root(), "KOMP_images", "Climb")

            username = input("Username: ")
            password = input("Password: ")
            token = climb.getTokens(username=username, password=password)
            json_objects = climb.getFileInfo(username=username, password=password, token=token, outputKey=658)
            fileLocationMap = climb.buildFileMap(json_objects)
            climb.download_from_drive(fileLocationMap=fileLocationMap, source=srcPath, target=targetPath)

        if downloadSource == "PFS" or "Core":

            # Connect to server of PFS, then extract information of images
            pass

    
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()


