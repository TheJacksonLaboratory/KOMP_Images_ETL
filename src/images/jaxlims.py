import collections
import datetime
import logging
import os
import shutil
from collections import defaultdict
from typing import Any

import mysql.connector
import paramiko

logger = logging.getLogger("__main__")


class image_upload_status(object):

    def __init__(self, DateOfUpload: datetime, UploadStatus: str, Message: str):
        self.DateOfUpload = DateOfUpload
        self.UploadStatus = UploadStatus
        self.Message = Message


def update_images_status(imageDict, imagefilekey):
    if not imageDict:
        raise ValueError("Nothing to be inserted")

    db_server = "rslims.jax.org"
    db_user = "dba"
    db_password = "rsdba"
    db_name = "komp"
    conn = db_init(server=db_server, username=db_user, password=db_password, database=db_name)
    cursor1 = conn.cursor()

    '''Remove deplciate records'''
    cleanStmt = """ DELETE i FROM komp.imagefileuploadstatus i, komp.imagefileuploadstatus j 
                    WHERE i._ImageFile_key > j._ImageFile_key AND i.SourceFileName = j.SourceFileName; 
                """

    cursor1.execute(cleanStmt)
    conn.commit()

    cursor2 = conn.cursor()
    sql = "UPDATE KOMP.imagefileuploadstatus SET {} WHERE _ImageFile_key = {};".format(
        ', '.join("{}='{}'".format(k, v) for k, v in imageDict.items()), imagefilekey)
    logger.debug(sql)
    cursor2.execute(sql)
    conn.commit()
    conn.close()


def db_init(server: str,
            username: str,
            password: str,
            database: str) -> mysql.connector:
    try:
        conn = mysql.connector.connect(host=server, user=username, password=password, database=database)
        return conn

    except mysql.connector.Error as err1:
        logger.error(err1)

    except ConnectionError as err2:
        logger.error(err2)

    return None


def buildFileMap(conn: mysql.connector.connection,
                 sql: str,
                 target: str) -> defaultdict[Any, list]:
    """

    :param conn: Connection to database
    :param sql: SQL query you want to execute
    :param target: Path you want to temporarily store the pictures
    :return: Dictionary after parsing the query result
    """
    if not conn:
        logger.error("No coonection")
        raise ConnectionError("Not connect to database")

    # fileLocations = []
    try:
        os.mkdir(target)

    except FileExistsError as e:
        print(e)

    '''Query database'''
    logger.info("Connecting to db")
    cursor = conn.cursor(buffered=True, dictionary=True)
    cursor.execute(sql)
    queryResult = cursor.fetchall()

    # Parse the data returned by query
    fileLocationMap = collections.defaultdict(list)
    for dict_ in queryResult:

        procedureCode = dict_["DestinationFileName"].split("/")[4]
        temp = dict_["SourceFileName"].split("\\")[4:]
        drive_path = "/Volumes/" # If you are on mac/linux
        fileLocation = os.path.join(drive_path, *temp)
        #fileLocation = "//" + os.path.join("bht2stor.jax.org\\", *temp).replace("\\", "/") #If you are on windows
        logger.debug(f"Source file path is {fileLocation}")

        fileLocationMap[procedureCode].append([int(dict_["_ImageFile_key"]), fileLocation])

        dest = target + "/" + procedureCode
        logger.debug(f"Destination of downloaded file is {dest}")

        try:
            os.mkdir(dest)

        except FileExistsError as e:
            print(e)

    # print(len(fileLocations))
    return fileLocationMap


def download_from_drive(fileLocationDict: defaultdict[list],
                        target: str) -> None:
    """
    :param fileLocationDict:Dictionary/hashmap that contains information of pictures file
    :param source: Base path of the file
    :param target: Path you want to temporarily store the pictures
    :return: None
    """

    if not fileLocationDict or not target:
        raise ValueError()

    for externalId, locations in fileLocationDict.items():

        logger.debug("Processing {}".format(externalId))
        for loc in locations:
            imagefileKey = loc[0]
            download_from = loc[1]
            download_to = target + "/" + externalId

            try:
                logger.info(f"Starting downloading {download_from} to {download_to}")
                shutil.copy(download_from, download_to)
                logger.info(f"Done downloading file {download_from}")
                fileName = loc[1].split("/")[-1]
                """Send downloaded files to the sever"""

                try:
                    ssh_client = paramiko.SSHClient()
                    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    ssh_client.connect(hostname="bhjlk02lp.jax.org", username="jlkinternal", password="t1m3st4mp!")
                    ftp_client = ssh_client.open_sftp()
                    ftp_client.chdir("/pictures/")

                    try:
                        logger.info(ftp_client.stat('/pictures/' + externalId + "/" + fileName))
                        logger.info(f'File exists in directory {externalId}')
                        file_Status = image_upload_status(DateOfUpload=datetime.datetime.now(),
                                                          UploadStatus="Success",
                                                          Message="File already exits on server")

                        update_images_status(file_Status.__dict__, imagefileKey)

                    except IOError:
                        logger.info(f"Uploading {fileName}")
                        ftp_client.put(download_to + "/" + fileName,
                                       "pictures/" + externalId + "/" + fileName)

                        file_Status = image_upload_status(DateOfUpload=datetime.datetime.now(),
                                                          UploadStatus="Success",
                                                          Message="File successfully uploaded to server")

                        update_images_status(file_Status.__dict__, imagefileKey)

                    # os.remove(os.path.join(download_to, loc[1].split("/")[-1]))
                    ftp_client.close()

                except paramiko.SSHException:
                    logger.error("Connection Error")

            except FileNotFoundError as e:
                # missingFiles.append(download_images.split("/"[-1]))
                logger.debug(download_from)
                logger.error(e)

                """Create object"""
                file_Status = image_upload_status(DateOfUpload=datetime.datetime.now(),
                                                  UploadStatus="Fail",
                                                  Message="File not found on the disk")

                update_images_status(file_Status.__dict__, imagefilekey=loc[0])
