import collections
import os
import shutil
from errno import errorcode
import mysql.connector
import paramiko
import requests
from requests import exceptions
import datetime
import logging

logger = logging.getLogger(__name__)


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
    logger.debug(f"Columns are {columns}")

    sql = "INSERT INTO %s ( %s ) VALUES ( %s );" % ("komp.imagefileuploadstatus", columns, placeholders)
    print(sql)
    logger.debug(sql)
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

    return None


def download_from_omero(username: str,
                        password: str,
                        download_to: str,
                        conn: mysql.connector.connection,
                        sql: str) -> None:
    """
    :param username: Your username of Omero.jax.org
    :param password: Your password of Omero.jax.org
    :param download_to: Path you want to temporarily store the images
    :param conn: Connection to the database
    :param sql: SQL query you want to use
    :return: None
    """

    if not download_to:
        return

    session = requests.Session()
    url = "https://omeroweb.jax.org/api/"
    response = session.get(url, verify=True)

    content = response.json()['data']
    print(content)
    forms = content[-1]

    base_url = forms['url:base']
    logger.debug(base_url)
    r = session.get(base_url)
    print(base_url)
    print(r.content)
    urls = r.json()
    servers_url = urls['url:servers']
    logger.debug(servers_url)

    """Get CSRF Token"""
    logger.info("Getting tokens")
    token_url = urls["url:token"]
    csrf = session.get(token_url).json()["data"]

    """List the servers available to connect to"""
    servers = session.get(servers_url).json()['data']
    servers = [s for s in servers if s['server'] == 'omero']
    if len(servers) < 1:
        raise Exception("Found no server called 'omero'")
    server = servers[0]
    print('server')
    print(server)
    logger.debug(server)

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
    logger.info("Logging in . . .")
    r = session.post(login_url, data=payload)
    print(r.content)
    login_rsp = r.json()

    try:
        r.raise_for_status()
    except exceptions.HTTPError as e:
        # Whoops it wasn't a 200
        print("Error {}".format(e))
        raise
    assert login_rsp['success']

    # Query the database
    urlMap = collections.defaultdict(list)
    cursor = conn.cursor(buffered=True, dictionary=True)
    cursor.execute(sql)
    # cursor.callproc('getImagesFrom', ['%omeroweb%'])
    queryResult = cursor.fetchall()
    # print(queryResult)
    # conn.commit()

    for dict_ in queryResult:
        for key, val in dict_.items():
            if key == "ExternalID":
                link = dict_["OutputValue"]
                testCode = dict_["TestCode"]
                urlMap[val].append((link, testCode))

    for key, val in urlMap.items():
        dest = download_to + "/" + key
        logging.info(dest)
        try:
            os.mkdir(dest)

        except FileExistsError as e:
            logger.error(e)
            #print(e)

        for pair in val:
            link, test_code = pair[0], pair[1]
            print(link)
            omeroId = link.split("/")[-1].strip()
            images_url = urls['url:images'] + str(omeroId)
            logger.debug(images_url)
            print(images_url)
            # get the filename from OMERO GET https://omeroweb.jax.org/api/v0/m/images/nnn - then get the "Name"
            # attribute of the response.
            resp = session.get(images_url)
            j = resp.json()
            name = j["data"]["Name"].strip()
            print(name)
            # File name has junk in it. Like " []". Needs a tif extension instead.
            frm, to = name.find("["), name.find("]")
            name = name.replace(name[frm:to + 1], "")
            name = name.strip()
            fName = str(test_code) + name
            logger.debug(f"Final file name is: {fName}")
            # logger.info(f"Download url is {fName}")

            downloadFileUrl = base_url.replace("api/v0/", "webgateway/archived_files/download/")
            downloadFileUrl = downloadFileUrl + str(omeroId)
            logger.info(f"Download url is {downloadFileUrl}")

            """Download files to target directory"""
            with session.get(downloadFileUrl, stream=True) as r:
                r.raise_for_status()
                with open(dest + "/" + fName, 'wb') as f:
                    logger.info("Downloading . . .")
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                f.close()

        """Push downloaded files to server"""
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        logger.info("Connecting to SFTP server . . .")
        ssh_client.connect(hostname="bhjlk02lp.jax.org", username="jlkinternal", password="t1m3st4mp!")
        ftp_client = ssh_client.open_sftp()
        images = os.listdir(dest)

        for image in images:
            destOnServer = os.path.join("images", key, image)
            logger.debug(f"Destination on server is: {destOnServer}")
            logger.debug(f"Uploading {image} to {destOnServer}")

            ftp_client.put(os.path.join(dest, image),
                           destOnServer)
            
            """Update the image file status table"""
            logger.info("Updating table now")
            file_Status = image_upload_status(SourceFileName="https://omeroweb.jax.org/api/v0/m/images/",
                                              DestinationFileName=os.path.join("", key, image),
                                              TaskKey=" ",
                                              DateOfUpload=datetime.datetime.now(),
                                              ImpcCode=" ",
                                              UploadStatus="Success")
            update_images_status(file_Status.__dict__)
            
        ftp_client.close()

        """Remove the directory after uploading"""
        logger.info("Empty the images directory")
        for filename in os.listdir(dest):
            file_path = os.path.join(dest, filename)
            logger.debug(file_path)
            try:
                logger.debug(f"Deleteing {file_path}")
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)

            except Exception as e:
                print('Failed to delete %s. Reason: %s' % (file_path, e))
