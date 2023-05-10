import images.omero as omr
import images.climb as climb
import images.jaxlims as jxl
import utils
import logging
import mysql.connector
import pandas as pd
import datetime
import os
from logging.handlers import RotatingFileHandler
import collections


"""
Algorithm

1. Query the database to get the files without 
"""

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


def process(srcFileName: str,
            conn: mysql.connector) -> None:
    """
    This function is intend to prepare for resubmitting images to DCC.
    It contains 3 parts:
    1). Read and parse the report(.csv file) of the failed images
    2). Use the information retrieved from the report to constructing sql queries to reset the status of submitted images
        back to "approved" (i.e. 14)
    3). Use the information retrieved from the report to constructing sql queries to query database for building file map,
        then add these queries to a list.

    :param srcFileName: file path to the dcc report
    :param conn: connection to the database
    :return: list of string with elements of sql queries
    """

    if not srcFileName:
        logger.error("No source file input!")
        return

    try:
        df = pd.read_csv(srcFileName)
        logger.info("Reading report . . .")
        #print(df)

        animalIds = df["animal_name"].tolist()
        impcCodes = df["procedure_key"].tolist()
        #print(animalIds)

        procedureInstanceKeys = []
        for impcCode, animalId in zip(impcCodes, animalIds):
            stmt = """SELECT _ProcedureInstance_key, '--', OrganismID, _LevelTwoReviewAction_key
                                    FROM
                                    ProcedureInstance
                                    INNER JOIN ProcedureInstanceOrganism USING (_ProcedureInstance_key)
                                    INNER JOIN Organism USING (_Organism_key)
                                    INNER JOIN ProcedureDefinitionVersion USING (_ProcedureDefinitionVersion_key)
                                    INNER JOIN ProcedureDefinition USING (_ProcedureDefinition_key)
                                    WHERE
                                    ProcedureDefinition.ExternalID = '{}' AND OrganismID ='{}'; 
                                """
            stmt = stmt.format(impcCode, animalId)
            logger.info("Querying the db . . .")
            cursor = conn.cursor(buffered=True, dictionary=True)
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
            cursor.execute(stmt)
            row = cursor.fetchall()
            print(row)
            conn.commit()

            try:
                logger.debug(f"Adding {row[0]['_ProcedureInstance_key']}")
                procedureInstanceKeys.append(row[0]["_ProcedureInstance_key"])

            except IndexError as error:
                logger.error(str(error) + f" with animal id {animalId} and IMPC code {impcCode} ")
        """Set the status """
        for procedureInstanceKey in procedureInstanceKeys:
            logger.debug(f"Updating status for {procedureInstanceKey}")
            stmt = """UPDATE ProcedureInstance SET _LevelTwoReviewAction_key = {} WHERE _ProcedureInstance_key = '{}';"""
            stmt = stmt.format(14, procedureInstanceKey)
            cursor = conn.cursor(buffered=True, dictionary=True)
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
            cursor.execute(stmt)
            conn.commit()

        queries = []

        logger.info("Constructing select statement for resubmission")
        

    except FileNotFoundError as e:
        logger.error(e)


def resubmit(stmt: str,
             conn: mysql.connector) -> None:
    """
    Function to iterate through the list of sql statements to form the sql queries that
    fetches file locations. Then download and upload it to the corresponding directories
    on the server.
    :param statements:
    :param conn:
    :return:
    """

    if not stmt:
        logger.error("No sql queries to be executed")

    if not conn:
        logger.error("Application is not connected to any database")

    
    targetPath = os.path.join(utils.get_project_root(), "KOMP_images", "JaxLims")
    fileLocationMap = jxl.buildFileMap(conn=conn, sql=stmt, target=targetPath)
    print(fileLocationMap)
    srcPath = "/Volumes/"  # If you are on mac/linux
    logger.info("Starts downloading . . .")
    #jxl.download_from_drive(fileLocationMap, source=srcPath, target=targetPath)
    logger.info("Done")

def main():

    print("Please tell me which QC report you want to use")
    #mediaFilesCsv = input("Media Report: ")
    #path_to_csv = os.path.join(utils.smbPath, mediaFilesCsv)
    path_to_csv = "/Volumes/phenotype/DccQcReports/J_QC_2023-04-19/J_failed_media_20230419.csv"
    logger.debug(f"Path to QC Report is: {path_to_csv}")

    server = "rslims.jax.org"
    username = "dba"
    password = "rsdba"
    database = "rslims"

    logger.info("Connecting to db...")
    conn = mysql.connector.connect(host=server, user=username, password=password, database=database)
    """
    1. Clean up the table 
    2. Read csv files and get organism id and impc code
    3. pass organism id and impc code to sql query to result
    4. Use functions in images module to download and upload
    5. Remove the duplicating records using set
    """

    process(srcFileName=path_to_csv, conn=conn)
    stmt = """SELECT * FROM KOMP.imagefileuploadstatus WHERE DATEDIFF(NOW(),DateCreated) < 7;"""
    resubmit(stmt=stmt, conn=conn)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
