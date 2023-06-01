import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

"""Function to get work directory"""


def get_project_root() -> Path:
    return Path(__file__).parent.parent


"""Setup logger"""


def createLogHandler(job_name, log_file):
    logger = logging.getLogger(__name__)
    FORMAT = "[%(asctime)s->%(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
    logging.basicConfig(format=FORMAT, filemode="w", level=logging.DEBUG, force=True)
    handler = logging.FileHandler(log_file)
    handler.setFormatter(logging.Formatter(FORMAT))
    logger.addHandler(handler)

    return logger


"""Omero/Climb username and password"""
username = "chent"
password = "Ql4nc,tzjzsblj."

"""SFTP server credentials"""
hostname = "bhjlk02lp.jax.org"
server_username = "jlkinternal"
server_password = "t1m3st4mp!"

"""Database credentials"""
db_server = "rslims.jax.org"
db_username = "dba"
db_password = "rsdba"
db_name = "rslims"

"""Disks location"""
smbPath = "/Volumes/phenotype/DccQcReports/"

"""SQL statement to get file location of an image"""
stmt = """SELECT * FROM KOMP.imagefileuploadstatus WHERE DateOfUpload IS NULL AND Message IS NULL;"""

'''
stmt = """SELECT ProcedureStatus, 
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
                                AND OutputValue LIKE '{where}' 
                                AND CHAR_LENGTH(OutputValue) > 0
                                AND Output.ExternalID IS NOT NULL
                                AND  DATEDIFF(NOW(),ProcedureInstance.DateModified ) < 14"""
'''
