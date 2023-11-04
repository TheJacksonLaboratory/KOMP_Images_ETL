import collections
from datetime import datetime
import shutil
from typing import Any
import mysql.connector
import os
import utils
import pandas as pd

# Function to merge two dictionaries
def Merge(dict1, dict2):
    for i in dict2.keys():
        dict1[i]=dict2[i]
    return dict1


# Function to get failed images from image upload table
def get_failed_images():
    STMT = """
            SELECT SourceFileName, TaskKey, Message 
                FROM komp.imagefileuploadstatus 
            WHERE UploadStatus = 'Fail';
        """
    conn = mysql.connector.connect(
                    host=db_server, 
                    user=db_username, 
                    password=db_password, 
                    database=db_name
                    )
    
    cursor = conn.cursor(buffered=True, dictionary=True)
    cursor.execute(STMT)
    db_records = cursor.fetchall()
    conn.close()
    return db_records

# Function to get related info of failed image, e.g. animal id of an image, procedure of image etc
def get_failed_images_info(missed_images_records):
    STMT = """
            SELECT OrganismID, ProcedureAlias FROM rslims.organism 
                INNER JOIN 
            rslims.procedureinstanceorganism 
                USING (_Organism_key) INNER JOIN 
            rslims.procedureinstance
                USING (_procedureinstance_key) INNER JOIN 
            proceduredefinitionversion
                USING (_proceduredefinitionversion_key) INNER JOIN 
            proceduredefinition
                USING (_proceduredefinition_key)
            WHERE 
                rslims.procedureinstanceorganism._ProcedureInstance_key = '{}';
        """
    conn = mysql.connector.connect(
                    host=db_server, 
                    user=db_username, 
                    password=db_password, 
                    database=db_name
                    )
    result = []
    for record in missed_images_records:
        task_key = record['TaskKey']
        cursor = conn.cursor(buffered=True, dictionary=True)
        cursor.execute(STMT.format(task_key))
        more_record = cursor.fetchall()[0]
        result.append(Merge(record, more_record))

    return result

# Function to write data to .csv file
def write_file(missed_images_info):
    if not missed_images_info:
        return
    
    date = datetime.now().strftime("%B-%d-%Y")
    filename = f"Missing_images_{date}.csv"
    df = pd.DataFrame(missed_images_info)
    df.to_csv(filename)



def main():
    missed_images = get_failed_images()
    missed_images_records = get_failed_images_info(missed_images)
    write_file(missed_images_info=missed_images_records)


if __name__ == '__main__':

    db_username = utils.db_username
    db_password = utils.db_password
    db_server = utils.db_server
    db_name = utils.db_name

    main()