import datetime
import logging
import os
import socket
import sys
from getpass import getpass
from logging.handlers import RotatingFileHandler

from urllib3.connection import HTTPConnection

import images.climb as climb
import images.jaxlims as jxl
import images.omero as omr
import utils

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

    print("Where do you want to download your pictures from?\n")
    downloadSource = input("Enter pictures source:")
    db_server = utils.db_server
    db_username = utils.db_username
    db_password = utils.db_password
    db_name = utils.db_name

    if downloadSource == "Omero":

        targetPath = os.path.join(utils.get_project_root(), "KOMP_images", "Omeros")
        logger.debug(f"Target path is {targetPath}")
        try:
            os.mkdir(targetPath)

        except FileExistsError as e:
            logger.warning("File/folder exists")

        conn = omr.db_init(server=db_server, username=db_username, password=db_password, database=db_name)
        sql = utils.stmt
        username = utils.username
        password = utils.password
        omr.download_from_omero(username=username, password=password, download_to=targetPath, conn=conn, sql=sql)
        conn.close()

    '''Download pictures in JaxLims'''
    if downloadSource == "JaxLims":
        conn = jxl.db_init(server=db_server, username=db_username, password=db_password, database=db_name)

        '''Remove duplciate records'''

        sql = utils.stmt("%phenotype%")
        sql = sql + ";"

        # List stores additional condition on sql query
        targetPath = os.path.join(utils.get_project_root(), "KOMP_images", "JaxLims")
        fileLocationMap = jxl.buildFileMap(conn=conn, sql=sql, target=targetPath)
        # srcPath = "//bht2stor.jax.org/"
        jxl.download_from_drive(fileLocationMap, target=targetPath)

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
        # Connect to server of PFS, then extract information of pictures
        pass


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
