import contextlib
import datetime
import glob
import logging
import os
import time
from pathlib import Path

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import mysql.connector
import pandas as pd
import utils
from sqlalchemy import create_engine


def get_project_root() -> Path:
    return Path(__file__).parent.parent


def createLogHandler(log_file) -> logging.Logger:
    logger = logging.getLogger(__name__)
    FORMAT = "[%(asctime)s->%(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
    logging.basicConfig(format=FORMAT, filemode="w", level=logging.DEBUG, force=True)
    handler = logging.FileHandler(log_file)
    handler.setFormatter(logging.Formatter(FORMAT))
    logger.addHandler(handler)

    return logger


class WatchPendingHandler(FileSystemEventHandler):
    """ Run a handler for every file added to the pending dir

    This class also handles what I call a bug in the watchdog module
    which means that you can get more than one call per real event
    in the watched dir tree.
    """

    def __init__(self):
        super(WatchPendingHandler, self).__init__()
        # wip is used to avoid bug in watchdog which means multiple calls
        # for one real event.
        # For reference: https://github.com/gorakhargosh/watchdog/issues/346
        self.wip = []

    def on_created(self, event) -> None:
        path = event.src_path
        if event.is_directory:
            logger.debug('WatchPendingHandler() New dir created in pending dir: {}'.format(path))
            return
        if path in self.wip:
            logger.debug('WatchPendingHandler() Dup created event for %s', path)
            return
        self.wip.append(path)
        logger.debug('WatchPendingHandler() New file created in pending dir: {}'.format(path))

    def on_moved(self, event) -> None:
        logger.debug('WatchPendingHandler() %s has been moved', event.src_path)
        with contextlib.suppress(ValueError):
            self.wip.remove(event.src_path)

    def on_deleted(self, event) -> None:
        path = event.src_path
        logger.debug('WatchPendingHandler() %s has been deleted', path)
        with contextlib.suppress(ValueError):
            self.wip.remove(path)

    @staticmethod
    def get_newly_added_file() -> str:
        list_of_files = glob.glob('/path/to/folder/*')  # * means all if need specific format then *.csv
        latest_file = max(list_of_files, key=os.path.getctime)
        logger.info(f"Newly added file is {latest_file}")
        return latest_file

    def on_handle(self, event) -> None:
        wk_file = self.get_newly_added_file()
        if os.path.isdir(wk_file):
            os.chdir(wk_file)
            for f in os.listdir(wk_file):
                if "failed_media" in wk_file and ".csv" in f:
                    logger.info(f"Stating handling {f}")
                    process(src_file_name=f)

        else:
            if "failed_media" in wk_file and ".csv" in wk_file:
                logger.info(f"Stating handling {wk_file}")
                process(src_file_name=wk_file)

            else:
                logger.error(f"Invalid file: {wk_file}")


def process(src_file_name: str) -> None:
    """
    This function is intend to prepare for resubmitting pictures to DCC.
    It contains 3 parts:
    1). Read and parse the report(.csv file) of the failed pictures
    2). Use the information retrieved from the report to constructing sql queries to reset the status of submitted pictures
        back to "approved" (i.e. 14)
    3). Use the information retrieved from the report to constructing sql queries to query database for building file map,
        then add these queries to a list.

    :param src_file_name: file path to the dcc report
    :return: None
    """

    if not src_file_name:
        logger.error("No source file input!")
        return

    # record = {}
    date = datetime.datetime.date()
    csv_root_dir = os.path.join(src_file_name.split("/").pop())
    fileName = src_file_name.split("/")[-1]
    record = {"DateOfHandling": date, "Location": csv_root_dir, "FileName": fileName}

    try:
        df = pd.read_csv(src_file_name)
        logger.info("Reading report . . .")

        animalIds = df["animal_name"].tolist()
        impcCodes = df["procedure_key"].tolist()

        procedureInstanceKeys = []
        logger.info("Connecting to db...")
        conn = mysql.connector.connect(host=db_server, user=db_username, password=db_password, database=db_name)
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
            stmt = """UPDATE ProcedureInstance SET _LevelTwoReviewAction_key = {} WHERE _ProcedureInstance_key = '{
            }';"""
            stmt = stmt.format(14, procedureInstanceKey)
            cursor = conn.cursor(buffered=True, dictionary=True)
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
            cursor.execute(stmt)
            conn.commit()

        conn.close()
        record["Status"] = "Done"
        insert_to_qc_table(db_record=record)
        logger.info("Process finished")

    except FileNotFoundError as e:
        logger.error(e)
        record["Status"] = "Fail"
        insert_to_qc_table(db_record=record)


def insert_to_qc_table(db_record: dict) -> None:
    if not db_record:
        logger.error("Nothing to be inserted")
        return

    data = pd.Series(db_record).to_frame()
    data = data.transpose()

    engine = create_engine("mysql+mysqlconnector://{0}:{1}@{2}/{3}".
                           format(db_username, db_password, db_server, db_name),
                           pool_recycle=3600,
                           pool_timeout=57600,
                           future=True)

    logger.debug(f"Insert {db_record} to database")
    data.to_sql("qc_reports", engine, schema="komp", if_exists='append', index=False, chunksize=1000)
    logger.info("Insertion done")


def main():
    """
    1. Clean up the table 
    2. Read csv files and get organism id and impc code
    3. pass organism id and impc code to sql query to result
    4. Use functions in pictures module to download and upload
    5. Remove the duplicating records using set
    """
    observer = Observer()
    observer.schedule(WatchPendingHandler(), root_dir, recursive=True)
    observer.start()
    logging.info('Watching %s', root_dir)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()

    # process(src_file_name=wk_dir)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    job_name = 'resubmit'
    logging_dest = os.path.join(utils.get_project_root(), "logs")
    date = datetime.datetime.now().strftime("%B-%d-%Y")
    logging_filename = logging_dest + "/" + f'{date}.log'
    logger = createLogHandler(logging_filename)
    logger.info('Logger has been created')

    root_dir = "/Volumes/phenotype/DccQcReports"
    logger.debug(f"Path to QC Report is: {root_dir}")

    db_username = utils.db_username
    db_password = utils.db_password
    db_server = utils.db_server
    db_name = utils.db_name

    main()
