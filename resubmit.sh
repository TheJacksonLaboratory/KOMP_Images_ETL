#!/bin/sh
chdir /Users/chent/Desktop/KOMP_Project/KOMP_Images_ETL

echo "Start to process resubmission"
python src/resubmit.py
echo "Preprocess done"

sleep 5s

echo "Resubmit failed images from phenotype drive"
python src/download_from_drive.py
echo "Job done"

sleep 5s

echo "Resubmit failed images from omero"
python src/download_from_omero.py
echo "Job done"