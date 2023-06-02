@echo off

ECHO Start to process resubmission
"C:\Program Files\Python311\python.exe" "C:\Program Files\KOMP\ImageDownload\KOMP_Images_ETL\src\resubmit.py"
ECHO Job done

timeout 5 > NUL

ECHO Uploading files from phenotype drive
"C:\Program Files\Python311\python.exe" "C:\Program Files\KOMP\ImageDownload\KOMP_Images_ETL\src\download_from_drive.py"
ECHO Job done

timeout 5 > NUL

ECHO Uploading files from OMERO
"C:\Program Files\Python311\python.exe" "C:\Program Files\KOMP\ImageDownload\KOMP_Images_ETL\src\download_from_omero.py"
ECHO Job done

@pause