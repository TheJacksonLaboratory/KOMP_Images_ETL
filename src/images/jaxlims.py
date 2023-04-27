import collections
import os
import shutil
from collections import defaultdict
from errno import errorcode
from typing import Any
import mysql.connector
import paramiko
import datetime


class image_upload_status(object):

    def __init__(self, SourceFileName: str, DestinationFileName: str, TaskKey: str,
                 DateOfUpload: datetime, ImpcCode: str, UploadStatus: str):
        self.SourceFileName = SourceFileName
        self.DestinationFileName = DestinationFileName
        self.TaskKey = TaskKey
        self.DateOfUpload = DateOfUpload
        self.ImpcCode = ImpcCode
        self.UploadStatus = UploadStatus


def update_images_status(imageDict: dict):
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

    placeholders = ', '.join(['%s'] * len(imageDict))
    columns = ', '.join(imageDict.keys())

    sql = "INSERT INTO %s ( %s ) VALUES ( %s );" % ("komp.imagefileuploadstatus", columns, placeholders)
    print(sql)
    cursor.execute(sql, list(imageDict.values()))
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
    fileLocationDict = collections.defaultdict(list)
    for dict_ in queryResult:
        procedureDef = dict_["ProcedureDefinition"].replace(" ", "")
        externalId = dict_["ExternalID"]
        pathToImage = dict_["OutputValue"].split("\\")

        dest = target + "/" + externalId
        try:
            os.mkdir(dest)

        except FileExistsError as e:
            print(e)

        """
        Handle different scenario of file path:
        1) Full path is entered, but there are some minor mistakes such as character, spelling etc
        2) Only file name is entered, which requires to form the path to the file in the drive 
        """

        # Case 1)
        if len(pathToImage) > 1:

            if "Phenotype" in pathToImage:
                # print(pathToImage)
                startIndex = pathToImage.index("Phenotype")
                imageLocation = pathToImage[startIndex:]
                print(imageLocation)
                fileLocationDict[externalId].append(os.path.join(*imageLocation).replace("\\", "/"))

            if "phenotype" in pathToImage:
                # print(pathToImage)
                startIndex = pathToImage.index("phenotype")
                imageLocation = pathToImage[startIndex:]
                print(imageLocation)
                # imageLocation = os.path.join(*imageLocation).replace("\\", "/")
                fileLocationDict[externalId].append(os.path.join(*imageLocation).replace("\\", "/"))
                print(fileLocationDict[externalId][-1])

            else:
                print(pathToImage)
        # Case 2)
        else:
            imageLocation = os.path.join("phenotype", procedureDef, "KOMP",
                                         "", dict_["OutputValue"])
            print(imageLocation)
            fileLocationDict[externalId].append(imageLocation.replace("\\", "/"))

    # print(len(fileLocations))
    return fileLocationDict


def download_from_drive(fileLocationDict: defaultdict[list],
                        source: str,
                        target: str) -> None:
    """
    :param fileLocationDict:Dictionary/hashmap that contains information of images file
    :param source: Base path of the file
    :param target: Path you want to temporarily store the images
    :return: None
    """

    if not fileLocationDict or not source \
            or not target:
        raise ValueError()

    for externalId, locations in fileLocationDict.items():
        for loc in locations:
            download_from = source + loc
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
                                                  DestinationFileName="images/" + externalId + loc.split("/")[-1],
                                                  TaskKey=" ",
                                                  DateOfUpload=datetime.datetime.now(),
                                                  ImpcCode=" ",
                                                  UploadStatus="Success")
                update_images_status(file_Status.__dict__)

            except FileNotFoundError as e:
                # missingFiles.append(download_images.split("/"[-1]))
                print(e)
                """Create object"""
                file_Status = image_upload_status(SourceFileName=download_from,
                                                  DestinationFileName=" ",
                                                  TaskKey=" ",
                                                  DateOfUpload=datetime.datetime.now(),
                                                  ImpcCode=" ",
                                                  UploadStatus="Fail")
                update_images_status(file_Status.__dict__)
