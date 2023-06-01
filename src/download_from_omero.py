import datetime
import logging
import os
import shutil
import time

import mysql.connector
import paramiko
import requests
from requests import exceptions

from src import utils


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


def update_images_status(record: dict,
                         imagefilekey: int):
    if not record:
        raise ValueError("Nothing to be inserted")

    conn = db_init(server=db_server, username=db_username, password=db_password, database=db_name)
    cursor = conn.cursor()

    '''Remove duplicate records'''

    cleanStmt = """ DELETE i FROM komp.imagefileuploadstatus i, komp.imagefileuploadstatus j 
                    WHERE i._ImageFile_key > j._ImageFile_key AND i.SourceFileName = j.SourceFileName; 
                """
    cursor.execute(cleanStmt)

    # placeholders = ', '.join(['%s'] * len(record))
    columns = ', '.join(record.keys())
    logger.debug(f"Columns are {columns}")

    sql = "UPDATE KOMP.imagefileuploadstatus SET {} WHERE _ImageFile_key = {};".format(
        ', '.join("{}='{}'".format(k, v) for k, v in record.items()), imagefilekey)
    print(sql)
    logger.debug(sql)
    cursor.execute(sql, list(record.values()))
    conn.commit()
    conn.close()


def download_from_omero(db_records: dict,
                        username: str,
                        password: str,
                        download_to) -> None:
    """
    :param username: Your username of Omero.jax.org
    :param password: Your password of Omero.jax.org
    :param download_to: Path you want to temporarily store the pictures
    :param db_records: SQL query you want to use
    :param download_to:
    :return: None
    """
    session = requests.Session()
    url = "https://omeroweb.jax.org/api/"
    response = session.get(url, verify=True)

    content = response.json()['data']
    forms = content[-1]

    base_url = forms['url:base']
    r = session.get(base_url)
    logger.debug(base_url)
    print(base_url)
    print(r.content)
    urls = r.json()
    servers_url = urls['url:servers']
    print(servers_url)

    """Get CSRF Token"""
    token_url = urls["url:token"]
    csrf = session.get(token_url).json()["data"]

    """List the servers available to connect to"""
    servers = session.get(servers_url).json()['data']
    servers = [s for s in servers if s['server'] == 'omero']
    if len(servers) < 1:
        raise Exception("Found no server called 'omero'")
    server = servers[0]

    """Log In To Omero"""
    login_url = urls['url:login']
    print(login_url)
    logger.debug(login_url)
    session.headers.update({'X-CSRFToken': csrf,
                            'Referer': login_url})
    payload = {'username': username,
               'password': password,
               'server': server['id']
               }
    r = session.post(login_url, data=payload)
    login_rsp = r.json()

    try:
        r.raise_for_status()
    except exceptions.HTTPError as e:
        # Whoops it wasn't a 200
        print("Error {}".format(e))
        raise
    assert login_rsp['success']

    for row in db_records:

        imageFileKey = row["_ImageFile_key"]
        omeroId = row["SourceFileName"].split("/")[-1]
        download_filename = omeroId + "_" + row["DestinationFileName"].split("/")[-1]
        logger.debug(f"Final file name is {download_filename}")
        print(download_filename)

        """Create a directory to temporally hold the downloaded files"""
        impc_code = row["DestinationFileName"].split("/")[4]
        temp = download_to + "/" + impc_code
        try:
            os.mkdir(temp)
        except FileExistsError as e:
            logger.error(e)

        downloadFileUrl = base_url.replace("api/v0/", "webgateway/archived_files/download/") + str(omeroId)
        print(downloadFileUrl)
        with session.get(downloadFileUrl, stream=True) as r:
            r.raise_for_status()
            with open(temp + "/" + download_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                f.close()

        time.sleep(3)

        if download_filename not in os.listdir(temp):
            logger.info(f"Unable to download file {download_filename}")
            file_Status = image_upload_status(DateOfUpload=datetime.datetime.now(),
                                              UploadStatus="Fail",
                                              Message="Fail to download file")

            update_images_status(record=file_Status.__dict__, imagefilekey=imageFileKey)

        else:
            logger.info(f"{download_filename} downloaded, ready to be sent to the server")
            send_to_server(file_to_send=download_filename,
                           hostname=hostname,
                           username=server_user,
                           password=server_password,
                           IMPC_Code=impc_code,
                           imageFileKey=imageFileKey)
            #
            time.sleep(3)


def send_to_server(file_to_send: str,
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
        ftp_client.chdir("/pictures/")

        try:
            logger.info(ftp_client.stat('/pictures/' + IMPC_Code + "/" + file_to_send))
            logger.info(f'File exists in directory {IMPC_Code}')
            file_Status = image_upload_status(DateOfUpload=datetime.datetime.now(),
                                              UploadStatus="Success",
                                              Message="File already exits on server")

            update_images_status(file_Status.__dict__, imageFileKey)

        except IOError:
            logger.info(f"Uploading {file_to_send}")
            ftp_client.put(download_to + "/" + file_to_send,
                           "pictures/" + IMPC_Code + "/" + file_to_send)

            file_Status = image_upload_status(DateOfUpload=datetime.datetime.now(),
                                              UploadStatus="Success",
                                              Message="File successfully uploaded to server")

            update_images_status(file_Status.__dict__, imageFileKey)

        # os.remove(os.path.join(download_to, loc[1].split("/")[-1]))
        ftp_client.close()

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
    stmt = utils.stmt
    cursor = conn.cursor(buffered=True, dictionary=True)
    cursor.execute(stmt)
    db_records = cursor.fetchall()
    download_from_omero(db_records=db_records, username=utils.username, password=utils.password,
                        download_to=download_to)

    conn.close()
    logger.info("Process finished")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    logger = logging.getLogger("__main__")

    db_username = utils.db_username
    db_password = utils.db_password
    db_server = utils.db_server
    db_name = utils.db_name

    download_to = "/Users/chent/Desktop/Pictures"

    hostname = utils.hostname
    server_user = utils.server_username
    server_password = utils.server_password

    main()
