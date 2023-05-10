import collections
import os
import shutil
from collections import defaultdict
from errno import errorcode
from typing import Any
import mysql.connector
import paramiko
import datetime
import logging

logger = logging.getLogger(__name__)

class image_upload_status(object):

    def __init__(self, SourceFileName: str, DestinationFileName: str, TaskKey: str,
                 DateOfUpload: datetime, ImpcCode: str, UploadStatus: str, Message:str):
        self.SourceFileName = SourceFileName
        self.DestinationFileName = DestinationFileName
        self.DateOfUpload = DateOfUpload
        self.UploadStatus = UploadStatus
        self.Message = Message


def update_images_status(imageDict: dict, imagefilekey):
    if not imageDict:
        raise ValueError("Nothing to be inserted")

    db_server = "rslims.jax.org"
    db_user = "dba"
    db_password = "rsdba"
    db_name = "komp"
    conn = db_init(server=db_server, username=db_user, password=db_password, database=db_name)
    cursor = conn.cursor()

    '''Remove deplciate records'''
    cleanStmt = """ DELETE i FROM komp.imagefileuploadstatus i, komp.imagefileuploadstatus j 
                    WHERE i._ImageFile_key > j._ImageFile_key AND i.SourceFileName = j.SourceFileName; 
                """

    cursor.execute(cleanStmt)

    logger.debug(f"Insert record data is {imageDict.values()}")
    sql = "UPDATE KOMP.imagefileuploadstatus SET {} WHERE _ImageFile_key = {};".format(
        ', '.join("{}='{}'".format(k, v) for k, v in imageDict.items()), imagefilekey)
    logger.debug(sql)
    print(sql)
    cursor.execute(sql)
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
        if err1.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Wrong user name or password passed")

        elif err1.errno == errorcode.ER_BAD_DB_ERROR:
            print("No such schema")

        else:
            error = str(err1.__dict__["orig"])
            print(error)

    except ConnectionError as err2:
        print(err2)

    return None


def buildFileMap(conn: mysql.connector.connection,
                 sql: str,
                 target: str) -> defaultdict[Any, list]:
    """

    :param conn: Connection to database
    :param sql: SQL query you want to execute
    :param target: Path you want to temporarily store the images
    :return: Dictionary after parsing the query result
    """
    if not conn:
        raise ConnectionError("Not connect to database")

    # fileLocations = []
    try:
        os.mkdir(target)

    except FileExistsError as e:
        print(e)

    '''Query database'''
    cursor = conn.cursor(buffered=True, dictionary=True)
    cursor.execute(sql)
    queryResult = cursor.fetchall()

    # Parse the data returned by query
    fileLocationMap = collections.defaultdict(list)
    for dict_ in queryResult:
        temp = dict_["SourceFileName"].replace("\\", "/").split("/")[2:]
        fileLocation = os.path.join("/Volumes", *temp)
        fileLocationMap[dict_["ImpcCode"]].append([int(dict_["_ImageFile_key"]), fileLocation])

        dest = target + "/" + dict_["ImpcCode"]
        try:
            os.mkdir(dest)

        except FileExistsError as e:
            print(e)

    # print(len(fileLocations))
    return fileLocationMap


def download_from_drive(fileLocationDict: defaultdict[list],
                        target: str) -> None:
    """
    :param fileLocationDict:Dictionary/hashmap that contains information of images file
    :param source: Base path of the file
    :param target: Path you want to temporarily store the images
    :return: None
    """

    if not fileLocationDict or not target:
        raise ValueError()

    for externalId, locations in fileLocationDict.items():
        for loc in locations:
            download_from = loc[1]
            imagefilekey = loc[0]
            download_to = target + "/" + externalId

            try:
                shutil.copy(download_from, download_to)

                """Send downloaded files to the sever"""
                ssh_client = paramiko.SSHClient()
                ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh_client.connect(hostname="bhjlk02lp.jax.org", username="jlkinternal", password="t1m3st4mp!")
                ftp_client = ssh_client.open_sftp()
                ftp_client.put(os.path.join(download_to, loc.split("/")[-1]),
                               "images/" + externalId + "/" + loc.split("/")[-1])
                os.remove(os.path.join(download_to, loc.split("/")[-1]))
                ftp_client.close()
                file_Status = image_upload_status(SourceFileName=download_from,
                                                  DestinationFileName="images/" + externalId + "/" + loc.split("/")[-1],
                                                  DateOfUpload=datetime.datetime.now(),
                                                  UploadStatus="Success",
                                                  Message="File successfully uploaded to server")
                
                update_images_status(file_Status.__dict__, imagefilekey)

            except FileNotFoundError as e:
                # missingFiles.append(download_images.split("/"[-1]))
                print(e)
                """Create object"""
                file_Status = image_upload_status(SourceFileName=download_from,
                                                  DestinationFileName="images/" + externalId + "/" + loc.split("/")[-1],
                                                  DateOfUpload=datetime.datetime.now(),
                                                  UploadStatus="Fail",
                                                  Message="File not found in the given location")
                
                update_images_status(file_Status.__dict__, imagefilekey)
