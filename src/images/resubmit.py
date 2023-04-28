import logging
import os
import socket
import sys
import datetime
from collections import defaultdict
from errno import errorcode
from getpass import getpass
from logging.handlers import RotatingFileHandler
from typing import Any
import mysql.connector
import paramiko
import requests
from requests import exceptions
from urllib3.connection import HTTPConnection
import pandas as pd

logger = logging.getLogger(__name__)
def getProcedureInstanceKeys(srcFileName: str,
                             conn: mysql.connector) -> list[str]:

    if not srcFileName:
        logger.error("No source file input!")
        return []

    cursor = conn.cursor(buffered=True, dictionary=True)
    try:
        df = pd.read_csv(srcFileName)
        logger.info("Reading report . . .")
        #print(df)

        animalIds = df["animal_name"].tolist()
        impcCodes = df["procedure_key"].tolist()
        #print(animalIds)

        selectStmt = """SELECT _ProcedureInstance_key, OrganismID, _LevelTwoReviewAction_key
                        FROM
                        ProcedureInstance
                        INNER JOIN ProcedureInstanceOrganism USING (_ProcedureInstance_key)
                        INNER JOIN Organism USING (_Organism_key)
                        INNER JOIN ProcedureDefinitionVersion USING (_ProcedureDefinitionVersion_key)
                        INNER JOIN ProcedureDefinition USING (_ProcedureDefinition_key)
                        WHERE
                        ProcedureDefinition.ExternalID = '{}' AND OrganismID IN ('{}'); "
                    """
        procedureInstanceKeys = []
        for impcCode, animalId in zip(impcCodes, animalIds):
            selectStmt = selectStmt.format(impcCode, animalId)
            #print(selectStmt)
            cursor.execute(selectStmt, multi=True)
            rows = cursor.fetchall()
            logger.debug(rows)

    except FileNotFoundError as e:
        logger.error(e)

    return []
def reset_status() -> None:
    pass


conn = mysql.connector.connect(host="rslims.jax.org", user="dba", password="rsdba", database="rslims")
procedureInstanceKeys = getProcedureInstanceKeys(srcFileName="/Volumes/phenotype/DccQcReports/J_QC_2023-04-19/J_failed_media_20230419.csv", conn=conn)
