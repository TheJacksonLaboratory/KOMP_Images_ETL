import collections
from datetime import datetime
import shutil
from typing import Any

import mysql.connector
import os
import utils
import paramiko

#  Globals
conn = None
cursor = None

def is_micro_ct(db_row: dict) -> bool:
    return  "IMPC_EMA_001" in db_row["ImpcCode"] or "IMPC_EMO_001" in db_row["ImpcCode"]


def get_micro_ct_impcCode(db_row: dict) -> str:
    if "IMPC_EMA_001" in db_row["ImpcCode"]:
        return "IMPC_EMA_001" 

    if "IMPC_EMO_001" in db_row["ImpcCode"]:
        return "IMPC_EMO_001"

def db():
    global conn
    if conn is None:
        conn = db_init()    
    
    return conn 
    

def db_init() -> mysql.connector:
    
    try:
        global conn
        conn = mysql.connector.connect(host=utils.db_server, user=utils.db_username, password=utils.db_password, database=utils.db_name)
        return conn 

    except mysql.connector.Error as err1:
        logger.error(err1)

    except ConnectionError as err2:
        logger.error(err2)

    return None

# Close the database connection
def db_close():
     # Close the database connection
     global conn, cursor    
     if conn is not None:
        conn.close()
        conn = None
                 

class image_upload_status(object):

    def __init__(self, DateOfUpload: datetime, UploadStatus: str, Message: str):
        self.DateOfUpload = DateOfUpload
        self.UploadStatus = UploadStatus
        self.Message = Message


def update_images_status(imageDict: dict, imagefilekey):
    if not imageDict:
        raise ValueError("Nothing to be inserted")
    
    cursor = conn.cursor()
    sql = "UPDATE KOMP.imagefileuploadstatus SET {} WHERE _ImageFile_key = {};".format(', '.join("{}='{}'".format(k,v) for k, v in imageDict.items()), imagefilekey)
    logger.debug(sql)
    cursor.execute(sql)
    db().commit()
    cursor.close() 


def generate_file_location(sql: str,
                           download_to: str) -> collections.defaultdict[Any, list]:
    
    '''Query database'''
    cursor = db().cursor(buffered=True, dictionary=True)
    cursor.execute(sql)
    db_records = cursor.fetchall()

    # Parse the data returned by query
    fileLocationMap = collections.defaultdict(list)
    for record in db_records:
 
        IMPC_Code = get_micro_ct_impcCode(db_row=record) if is_micro_ct(db_row=record) else record["DestinationFileName"].split("/")[4]
        source_file_name = record["SourceFileName"].split("\\")
        image_file_key = record["_ImageFile_key"]
        destFileName = record["DestinationFileName"].split("/")[-1]
        #Case when no specific image name is provided, e.g \\\\jax\\jax\\phenotype\\SHIRPA\\KOMP\\images\\ in column "SourceFileName"
        if source_file_name[-1] == '':
            file_Status = image_upload_status(DateOfUpload=datetime.today().strftime('%Y-%m-%d'),
                                              UploadStatus="Failed",
                                              Message="Misformatted image name")

            update_images_status(file_Status.__dict__, image_file_key)
            continue
        testcode = record["DestinationFileName"].split("/")[-1].split("_")[0] if not is_micro_ct(db_row=record) else ""
        temp = record["SourceFileName"].split("\\")[4:]
        fileLocation = "//" + os.path.join("bht2stor.jax.org\\", *temp).replace("\\", "/") #If you are on windows
        logger.debug(f"Source file path is {fileLocation}")

        fileLocationMap[IMPC_Code].append([int(record["_ImageFile_key"]), fileLocation, testcode, destFileName])

        download_to_dest = download_to + "/" + IMPC_Code
        logger.debug(f"Destination of downloaded file is {download_to_dest}")

        try:
            os.mkdir(download_to_dest)

        except FileExistsError as e:
            print(e)

    cursor.close()  
    return fileLocationMap


def download_from_drive(fileLocationDict: collections.defaultdict[list],
                        target: str) -> None:
    

    if not fileLocationDict or not target:
        raise ValueError()

    for IMPC_Code, locations in fileLocationDict.items():

        logger.debug("Processing {}".format(IMPC_Code))
        for loc in locations:
            imagefileKey = loc[0]
            download_from = loc[1]
            #testcode = loc[2]  # Obsolete
            destFileName = loc[3]
            download_to_dest = target + "/" + IMPC_Code
            
            fileName = loc[1].split("/")[-1]
            logger.info(f"Starting downloading file {fileName} from {download_from} to {download_to_dest} as {destFileName}")
            try:
                shutil.copy(download_from, download_to_dest)
                logger.info(f"Done downloading file {fileName}")

                #Send downloaded files to the sever
                logger.info(f"Start to send file {fileName}")
                send_to_server(file_to_send=fileName,
                                dest_file_name=destFileName,
                                IMPC_Code=IMPC_Code,
                                imageFileKey=imagefileKey)
                DFS(dir_to_remove=download_to_dest)
            
            except FileNotFoundError as e:
                logger.error(e)

                file_Status = image_upload_status(DateOfUpload=datetime.today().strftime('%Y-%m-%d'),
                                                  UploadStatus="Fail",
                                                  Message="File not found on the disk")

                update_images_status(file_Status.__dict__, imagefilekey=imagefileKey)
            

               


def send_to_server(file_to_send: str,
                   dest_file_name: str,
                   IMPC_Code: str,
                   imageFileKey: int) -> None:
    
    try:
        # Got rid of testcode messiness
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=utils.hostname, username=utils.server_username, password=utils.server_password)
        ftp_client = ssh_client.open_sftp()
        try:
            logger.info(ftp_client.stat('/images/' + IMPC_Code + "/" + dest_file_name))
            logger.info(f'File {dest_file_name} exists in directory {IMPC_Code}')
            file_Status = image_upload_status(DateOfUpload=datetime.today().strftime('%Y-%m-%d'),
                                              UploadStatus="Success",
                                              Message="File already exits on server")

            update_images_status(file_Status.__dict__, imageFileKey)

        except IOError:
            logger.info(f"Uploading {file_to_send}")
            ftp_client.put(download_to + "/" + IMPC_Code + "/" + file_to_send,
                           "images/" + IMPC_Code + "/" + dest_file_name)

            file_Status = image_upload_status(DateOfUpload=datetime.today().strftime('%Y-%m-%d'),
                                              UploadStatus="Success",
                                              Message="File successfully uploaded to server")

            update_images_status(file_Status.__dict__, imageFileKey)

        logger.info(f"Finish uploading {file_to_send}")
        ftp_client.close()  
    except paramiko.SSHException:
        logger.error("Connection Error")

def remove_duplicates():
    cursor = db().cursor()
    cursor.execute(utils.delete_stmt)
    db().commit()
    cursor.close()  
    
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
    
    try:
        db_init()
        stmt = utils.pheno_stmt
        fileLocationDict = generate_file_location(sql=stmt, download_to=download_to)
        download_from_drive(fileLocationDict=fileLocationDict, target=download_to)
    except Exception as e:
        logger.error(e)     
    finally:
        # remove_duplicates()  # Do we care about duplicates in komp.imagefileuploadstatus?
        db_close()

    logger.info("Job has been completed")
    
# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    job_name = 'download_from_drive'
    logging_dest = os.path.join(utils.get_project_root(), "logs")
    date = datetime.now().strftime("%B-%d-%Y")
    logging_filename = logging_dest + "/" + f'{date}.log'
    logger = utils.createLogHandler(job_name ,logging_filename )
    logger.info('Logger has been created')

    #download_to = "C:/Program Files/KOMP/ImageDownload/pictures"
    download_to = utils.download_to
    try:
        os.mkdir(download_to)

    except FileExistsError as e:
        logger.error(e)
        
    main()
