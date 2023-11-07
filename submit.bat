@echo off

ECHO Uploading images from Omero
"C:\Program Files\Python311\python.exe" "C:\Program Files\KOMP\ImageDownload\KOMP_Images_ETL\src\download_from_omero.py"
ECHO Job done

timeout 5 > NUL

ECHO Uploading files from phenotype drive
"C:\Program Files\Python311\python.exe" "C:\Program Files\KOMP\ImageDownload\KOMP_Images_ETL\src\download_from_drive.py"
ECHO Job done

ECHO Creating missing images report
"C:\Program Files\Python311\python.exe" "C:\Program Files\KOMP\ImageDownload\KOMP_Images_ETL\src\report_missing_images.py"
@pause