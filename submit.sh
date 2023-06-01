#!/bin/sh
chdir /Users/chent/Desktop/KOMP_Project/KOMP_Images_ETL

echo "Downloading files from phenotype drive"
python src/download_from_drive.py
echo "Job done"

sleep 5s

echo "Download files from Omero.jax.org"
python src/download_from_omero.py
echo "Job done"