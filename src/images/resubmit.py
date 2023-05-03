import logging
import mysql.connector
import pandas as pd
import collections

logger = logging.getLogger(__name__)


def process(srcFileName: str,
            conn: mysql.connector) -> list[str]:
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
        return []

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
        for impcCode, animalId in zip(impcCodes, animalIds):
            resubmit_select_stmt = """SELECT ProcedureStatus, 
                                                ProcedureDefinition, 
                                                ProcedureDefinition.ExternalID AS ExternalID, 
                                                _ProcedureInstance_key AS TestCode, 
                                                OutputValue, DateDue,
                                                OrganismID,
                                                DateBirth,
                                                StockNumber
                                            FROM
                                                Organism
                                                    INNER JOIN
                                                ProcedureInstanceOrganism USING (_Organism_key)
                                                    INNER JOIN
                                                ProcedureInstance USING (_ProcedureInstance_key)
                                                    INNER JOIN
                                                OutputInstanceSet USING (_ProcedureInstance_key)
                                                    INNER JOIN
                                                Outputinstance USING (_outputInstanceSet_key)
                                                    INNER JOIN
                                                Output USING (_Output_key)
                                                    INNER JOIN
                                                ProcedureDefinitionVersion USING (_ProcedureDefinitionVersion_key)
                                                    INNER JOIN
                                                ProcedureDefinition USING (_ProcedureDefinition_key)
                                                    INNER JOIN
                                                Line USING (_Line_key)
                                                    INNER JOIN
                                                OrganismStudy USING (_Organism_key)
                                                    INNER JOIN
                                                cv_ProcedureStatus USING (_ProcedureStatus_key)
                                            WHERE
                                                Output._DataType_key = 7   -- File type
                                                    AND OutputValue LIKE '%phenotype%' 
                                                    AND CHAR_LENGTH(OutputValue) > 0
                                                    AND Output.ExternalID IS NOT NULL
                                                    AND OrganismID = '{}'
                                                    AND ProcedureDefinition.ExternalID = '{}';"""
            resubmit_select_stmt  = resubmit_select_stmt.format(animalId, impcCode)
            print(resubmit_select_stmt)
            queries.append(resubmit_select_stmt)

        return queries



    except FileNotFoundError as e:
        logger.error(e)


def getImageInfo(srcFileName: str) -> collections.defaultdict:

    try:
        result = collections.defaultdict(list)
        df = pd.read_csv(srcFileName)
        logger.info("Reading report . . .")
        animalIds = df["animal_name"].\
            tolist()
        impcCodes = df["procedure_key"].tolist()
        result["animalIds"] = animalIds
        result["externalId"] = impcCodes

        return result

    except FileNotFoundError as e:
        logger.error(e)


def reset_status(conn: mysql.connector,
                 procedureInstanceKeys: list[str], 
                 statusKey: int) -> None:
    """
    Function to reset the status of media files from "Submitted" to "Approved"
    :param conn:
    :param procedureInstanceKeys:
    :param statusKey:
    :return: None
    """
    if not conn:
        logger.error("DB not connected")
        return
    
    if not procedureInstanceKeys:
        logger.error(f"Empty list: {procedureInstanceKeys}")
        raise ValueError("Index out of range")
    
    stmt = """UPDATE ProcedureInstance SET _LevelTwoReviewAction_key = {} WHERE _ProcedureInstance_key = '{}';"""
    for procedureInstanceKey in procedureInstanceKeys:
        logger.debug(f"Updating status for {procedureInstanceKey}")
        stmt = stmt.format(statusKey, procedureInstanceKey)
        cursor = conn.cursor(buffered=True, dictionary=True)
        cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        cursor.execute(stmt)
        conn.commit()


'''
conn = mysql.connector.connect(host="rslims.jax.org", user="dba", password="rsdba", database="rslims")
procedureInstanceKeys = getProcedureInstanceKeys(srcFileName="/Volumes/phenotype/DccQcReports/J_QC_2023-04-19/J_failed_media_20230419.csv", conn=conn)
'''