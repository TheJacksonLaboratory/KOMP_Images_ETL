from pathlib import Path


server = "rslims.jax.org"
username = "dba"
password = "rsdba"
database = "rslims"

def get_project_root() -> Path:
    return Path(__file__).parent.parent