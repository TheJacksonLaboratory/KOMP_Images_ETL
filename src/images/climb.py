import collections
import os.path
import shutil
from collections import defaultdict
from errno import errorcode
from typing import Any

import mysql.connector
import paramiko
import requests
import datetime
import logging

logger = logging.getLogger("Core")

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
    """
    :param server: Host of the database
    :param username: Username to log in to db
    :param password: Password to log in to db
    :param database: Schema you want to connect to
    :return: mysql.Connection object
    """
    try:
        logger.info("Connecting to database . . .")
        conn = mysql.connector.connect(host=server, user=username, password=password, database=database)
        return conn

    except mysql.connector.Error as err1:
        if err1.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logger.error("Wrong user name or password passed")

        elif err1.errno == errorcode.ER_BAD_DB_ERROR:
            logger.error("No such schema")

        else:
            error = str(err1.__dict__["orig"])
            logger.error(error)

    except ConnectionError as err2:
        logger.error(err2)



"""Function to get tokens for signing in"""


def getTokens(username: str,
              password: str) -> str:
    """
    :param username: Username of yours to sign in to Climb
    :param password: Password of yours to sign in to Climb
    return: Tokens for authenticating
    """

    if not username or not password:
        raise ValueError("Invalid Credentials")

    try:
        tokenUrl = 'http://climb-admin.azurewebsites.net/api/token'
        token = requests.get(tokenUrl, auth=(username, password), timeout=15).json()["access_token"]
        logger.info("Token got")
        return token

    except TimeoutError as e:
        logger.error(e.__dict__)


#######################################################################################

def getFileInfo(username: str, password: str, token: str, outputKey: int) -> list[dict]:
    """
    :param username: Username of yours to sign in to Climb
    :param password: Password of yours to sign in to Climb
    :param token: Tokens for authenticating
    :param outputKey: Output key of an experiment/assay

    return: Json object that stores metadata of images associate with the given output key

    The function contains 3 parts: 1). Use token to see how many workgroup we are in. 2). Find the right workgroup,
    in this case, is "KOMP-JAX Lab" 3). Update and switch to the right workgroup, then get a new token for
    authentication purpose, pull back data use the new token.

    """
    if not username or not password or not token:
        raise ValueError("Invalid Credentials")

    result = []
    base_url = "https://api.climb.bio/api/"
    call_header = {'Authorization': 'Bearer ' + token}
    logger.debug(base_url + "workgroups")
    wgResponse = requests.get(base_url + 'workgroups', headers=call_header, timeout=15)
    wgJson = wgResponse.json()

    """Check for workgroup"""
    total_item_count = wgJson.get('totalItemCount')
    logger.debug(f"Number of workgroups:{total_item_count}")
    print(total_item_count)
    outer_dict = wgJson.get('data')

    # Get a list workgroups
    dict_list = outer_dict.get('items')
    logger.debug(dict_list)

    """Switch to the right workgroup and pull data back"""
    for dict_ in dict_list:
        wkgName = dict_["workgroupName"]
        logger.debug(f"Work Group: {wkgName}")

        if wkgName == "KOMP-JAX Lab":
            call_header = {'Authorization': 'Bearer ' + token}
            status_code = requests.put(base_url + 'workgroups/' + str(dict_['workgroupKey']),
                                       headers=call_header)
            logger.debug(status_code)

            # Get a new token
            newToken = getTokens(username=username, password=password)
            url = f"https://api.climb.bio/api/taskinstances/taskOutputs?OutputKey={str(outputKey)}"
            logger.debug(url)

            # Pull back data
            payload = {}
            headers = {'Authorization': 'Bearer ' + newToken}
            response = requests.request("GET", url, headers=headers, data=payload)
            print(response.json())
            logger.debug(f"Adding {response.json()['data']['items']}")
            result.append(response.json()['data']['items'])

    return result


####################################################################################

def buildFileMap(json_objects: list[dict]) -> defaultdict[Any, Any]:
    """
    Construct a dictionary like this: IMPC_CODE -> (taskInstanceKey, filename)
    """
    if not json_objects:
        raise ValueError("No associate file found!")

    """Query the database"""
    db_server = "rslims.jax.org"
    db_user = "dba"
    db_password = "rsdba"
    db_name = "komp"
    conn = db_init(server=db_server, username=db_user, password=db_password, database=db_name)
    sql = "SELECT ProcedureCode, ImpcCode  FROM komp.dccparameterdetails WHERE _ClimbType_key = 658;"
    cursor = conn.cursor(buffered=True, dictionary=True)
    cursor.execute(sql)
    queryResult = cursor.fetchall()
    print(queryResult)
    
    fileLocationMap = collections.defaultdict(list)
    for json_obj in json_objects[0]:
        pair = [json_obj["taskInstanceKey"], json_obj["outputValue"], json_obj["taskOutputKey"],
                queryResult[0]["ImpcCode"]]
        # pair = [json_obj["taskInstanceKey"], json_obj["outputValue"]]
        # print(pair)
        """Parse the db query result"""
        fileLocationMap[queryResult[0]["ProcedureCode"]].append(pair)

    return fileLocationMap


######################################################################

def download_from_drive(fileLocationMap: defaultdict[list],
                        source: str,
                        target: str) -> None:
    """
    :param fileLocationMap:Dictionary/Hashmap that contains information of images file
    :param source: Base path of the file
    :param target: Path you want to temporarily store the images
    :return: None
    """
    if not fileLocationMap:
        raise ValueError("Empty Dict found")

    if not source or not target:
        raise ValueError("No file source or target")

    for externalId, pairs in fileLocationMap.items():
        dest = os.path.join(target, externalId)
        try:
            os.mkdir(dest)

        except FileExistsError as err:
            print(err)

        for pair in pairs:
            taskInstanceKey, fileLocation = pair[0], pair[1]
            taskKey, impcCode = pair[2], pair[3]

            if not fileLocation:
                print("No file path found")
                """Create object"""
                file_Status = image_upload_status(SourceFileName=" ",
                                                  DestinationFileName=" ",
                                                  TaskKey=" ",
                                                  DateOfUpload=datetime.datetime.now(),
                                                  ImpcCode=" ",
                                                  UploadStatus="Fail")
                update_images_status(file_Status.__dict__)

            else:
                """Form the file name"""
                fileName = str(taskInstanceKey) + "_" + fileLocation.split("\\")[-1]
                print(fileName)

                """Reformat the file location"""
                fileLocation = os.path.join(*(fileLocation.split("\\")[5:]))
                fileLocation = source + fileLocation
                print(fileLocation)

                """Download files from the drive"""
                download_from = os.path.join(source, fileLocation)
                print(download_from)
                download_to = os.path.join(dest, fileName)
                #download_to = download_to.replace("\\", "/")
                print(download_to)

                try:
                    shutil.copy(download_from, download_to)

                    """Send downloaded files to the sever"""
                    ssh_client = paramiko.SSHClient()
                    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    ssh_client.connect(hostname="bhjlk02lp.jax.org", username="jlkinternal", password="t1m3st4mp!")
                    ftp_client = ssh_client.open_sftp()
                    print(os.path.join("", externalId, fileName))
                    ftp_client.put(download_to,
                                   "images" + "/" + externalId + "/" + fileName)

                    file_Status = image_upload_status(SourceFileName=fileLocation,
                                                      DestinationFileName=os.path.join("", externalId, fileName),
                                                      TaskKey=str(taskKey),
                                                      DateOfUpload=datetime.datetime.now(),
                                                      ImpcCode=impcCode,
                                                      UploadStatus="Success")
                    update_images_status(file_Status.__dict__)

                    """Clean up the local directory after the file is uploaded"""
                    # os.remove(os.path.join(download_to, fileName))
                    ftp_client.close()

                except FileNotFoundError as e:
                    print(e)

