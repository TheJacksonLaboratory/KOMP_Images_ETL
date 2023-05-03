from pathlib import Path

"""Database credentials"""
server = "rslims.jax.org"
username = "dba"
password = "rsdba"
database = "rslims"

"""Disks location"""
smbPath = "/Volumes/phenotype/DccQcReports/"

"""SQL statement to call"""
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



"""Function to get work directory"""
def get_project_root() -> Path:
    return Path(__file__).parent.parent