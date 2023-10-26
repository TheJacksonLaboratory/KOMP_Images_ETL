import collections
from datetime import datetime
import shutil
from typing import Any

import mysql.connector
import os
import utils
import paramiko


def is_micro_ct(db_row: dict) -> bool:
    return  "IMPC_EMA_001" in db_row["ImpcCode"] or "IMPC_EMO_001" in db_row["ImpcCode"]


def get_micro_ct_impcCode(db_row: dict) -> str:
    if "IMPC_EMA_001" in db_row["ImpcCode"]:
        return "IMPC_EMA_001" 

    if "IMPC_EMO_001" in db_row["ImpcCode"]:
        return "IMPC_EMO_001"
    

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
        logger.error(err1)

    except ConnectionError as err2:
        logger.error(err2)

    return None


class image_upload_status(object):

    def __init__(self, DateOfUpload: datetime, UploadStatus: str, Message: str):
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
    cursor1 = conn.cursor()

    
    '''Remove deplciate records'''
    cleanStmt = """ DELETE i FROM komp.imagefileuploadstatus i, komp.imagefileuploadstatus j 
                    WHERE i._ImageFile_key < j._ImageFile_key AND i.SourceFileName = j.SourceFileName; 
                """
    
    cursor1.execute(cleanStmt)
    cursor2 = conn.cursor()
    sql = "UPDATE KOMP.imagefileuploadstatus SET {} WHERE _ImageFile_key = {};".format(', '.join("{}='{}'".format(k,v) for k, v in imageDict.items()), imagefilekey)
    logger.debug(sql)
    print(sql)
    cursor2.execute(sql)
    conn.commit()
    conn.close()


def generate_file_location(conn: mysql.connector.connection,
                           sql: str,
                           download_to: str) -> collections.defaultdict[Any, list]:
    """

    :param conn: Connection to database
    :param sql: SQL query you want to execute
    :param target: Path you want to temporarily store the pictures
    :return: Dictionary after parsing the query result
    """
    if not conn:
        logger.error("No coonection")
        raise ConnectionError("Not connect to database")

    '''Query database'''
    # logger.info("Connecting to db")
    cursor = conn.cursor(buffered=True, dictionary=True)
    cursor.execute(sql)
    db_records = cursor.fetchall()

    # Parse the data returned by query
    fileLocationMap = collections.defaultdict(list)
    for record in db_records:
 
        IMPC_Code = get_micro_ct_impcCode(db_row=record) if is_micro_ct(db_row=record) else record["DestinationFileName"].split("/")[4]
        testcode = record["DestinationFileName"].split("/")[-1].split("_")[0] if not is_micro_ct(db_row=record) else ""
        temp = record["SourceFileName"].split("\\")[4:]
        fileLocation = "//" + os.path.join("bht2stor.jax.org\\", *temp).replace("\\", "/") #If you are on windows
        logger.debug(f"Source file path is {fileLocation}")

        fileLocationMap[IMPC_Code].append([int(record["_ImageFile_key"]), fileLocation, testcode])

        download_to_dest = download_to + "/" + IMPC_Code
        logger.debug(f"Destination of downloaded file is {download_to_dest}")

        try:
            os.mkdir(download_to_dest)

        except FileExistsError as e:
            print(e)

    # print(len(fileLocations))
    return fileLocationMap


def download_from_drive(fileLocationDict: collections.defaultdict[list],
                        target: str) -> None:
    """
    :param fileLocationDict:Dictionary/hashmap that contains information of pictures file
    :param source: Base path of the file
    :param target: Path you want to temporarily store the pictures
    :return: None
    """

    if not fileLocationDict or not target:
        raise ValueError()

    for IMPC_Code, locations in fileLocationDict.items():

        logger.debug("Processing {}".format(IMPC_Code))
        for loc in locations:
            imagefileKey = loc[0]
            download_from = loc[1]
            testcode = loc[2]
            download_to_dest = target + "/" + IMPC_Code

            fileName = loc[1].split("/")[-1]
            logger.info(f"Starting downloading file {fileName} from {download_from} to {download_to_dest}")
            try:
                shutil.copy(download_from, download_to_dest)
                logger.info(f"Done downloading file {fileName}")

                """Send downloaded files to the sever"""
                logger.info(f"Start to send file {fileName} to {hostname}")
                send_to_server(file_to_send=fileName,
                               testcode=testcode,
                                hostname=hostname,
                                username=server_user,
                                password=server_password,
                                IMPC_Code=IMPC_Code,
                                imageFileKey=imagefileKey)
                DFS(dir_to_remove=download_to_dest)
            
            except FileNotFoundError as e:
                # missingFiles.append(download_images.split("/"[-1]))
                logger.error(e)

                """Create object"""
                file_Status = image_upload_status(DateOfUpload=datetime.today().strftime('%Y-%m-%d'),
                                                  UploadStatus="Fail",
                                                  Message="File not found on the disk")

                update_images_status(file_Status.__dict__, imagefilekey=imagefileKey)
            

               


def send_to_server(file_to_send: str,
                   testcode: str,
                   hostname: str,
                   username: str,
                   password: str,
                   IMPC_Code: str,
                   imageFileKey: int) -> None:
    """

    """
    try:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=hostname, username=username, password=password)
        ftp_client = ssh_client.open_sftp()
        #ftp_client.chdir("/images/")
        final_filename = testcode + "_" + file_to_send if testcode else file_to_send
        try:
            logger.info(ftp_client.stat('/images/' + IMPC_Code + "/" + final_filename))
            logger.info(f'File {final_filename} exists in directory {IMPC_Code}')
            file_Status = image_upload_status(DateOfUpload=datetime.today().strftime('%Y-%m-%d'),
                                              UploadStatus="Success",
                                              Message="File already exits on server")

            update_images_status(file_Status.__dict__, imageFileKey)

        except IOError:
            logger.info(f"Uploading {final_filename}")
            ftp_client.put(download_to + "/" + IMPC_Code + "/" + file_to_send,
                           "images/" + IMPC_Code + "/" + final_filename)

            file_Status = image_upload_status(DateOfUpload=datetime.today().strftime('%Y-%m-%d'),
                                              UploadStatus="Success",
                                              Message="File successfully uploaded to server")

            update_images_status(file_Status.__dict__, imageFileKey)

        # os.remove(os.path.join(download_to, loc[1].split("/")[-1]))
        ftp_client.close()
        logger.info(f"Finish uploading {file_to_send}")

    except paramiko.SSHException:
        logger.error("Connection Error")


def DFS(dir_to_remove: str) -> None:
    if not dir_to_remove:
        logger.error("No input directory")
        return

    for filename in os.listdir(dir_to_remove):
        file_path = os.path.join(dir_to_remove, filename)
        logger.debug(file_path)
        try:
            logger.debug(f"Deleteing {file_path}")
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)

        except Exception as e:
            logger.warning('Failed to delete %s. Reason: %s' % (file_path, e))


def main():
    conn = db_init(server=db_server, username=db_username, password=db_password, database=db_name)
    stmt = utils.pheno_stmt

    fileLocationDict = generate_file_location(conn=conn, sql=stmt, download_to=download_to)
    download_from_drive(fileLocationDict=fileLocationDict, target=download_to)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    job_name = 'download_from_drive'
    logging_dest = os.path.join(utils.get_project_root(), "logs")
    date = datetime.now().strftime("%B-%d-%Y")
    logging_filename = logging_dest + "/" + f'{date}.log'
    logger = utils.createLogHandler(job_name ,logging_filename )
    logger.info('Logger has been created')

    db_username = utils.db_username
    db_password = utils.db_password
    db_server = utils.db_server
    db_name = utils.db_name

    download_to = "C:/Program Files/KOMP/ImageDownload/pictures"
    try:
        os.mkdir(download_to)

    except FileExistsError as e:
        logger.error(e)

    hostname = utils.hostname
    server_user = utils.server_username
    server_password = utils.server_password

    main()
