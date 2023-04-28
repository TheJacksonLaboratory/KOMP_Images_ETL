from pathlib import Path


server = "rslims.jax.org"
username = "dba"
password = "rsdba"
database = "rslims"

smbPath = "/Volumes/phenotype/DccQcReports"
def get_project_root() -> Path:
    return Path(__file__).parent.parent