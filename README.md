# ETL For KOMP Images

## Description

This project is intended to download, upload images files from multiple sources including Omero, Climb and PFS to SFTP Server(for submitting to DCC). The backgroud of this project is that the old image uploading application broke recently, and it has been running for over 10 years, making a new app will be easier for engineers to use and maintain in the future. As of today, Omero and Climb part have been finished and well-tested, PFS part will be soon as the KOMP team switch from JaxLims to PFS. 

<br>
<br>

## Table of Contents

If your README is long, add a table of contents to make it easy for users to find what they need.

- [Installation](#installation)
- [Usage](#usage)
- [Contributors](#credits)


<br>
<br>

## Installation

Assume that you alhready have the python install, first, create a virtual environment for running the application, I use `venv`, you can pick whatever suites you. 

```
python -m venv .env/ENV_NAME
```
After creating the enviroment, activate it use the following coomand 

```
. . env/ENV_NAME/bin/activate
```

Then use the following command to install the package to the `venv` you just created:

```
 pip install git+https://github.com/TheJacksonLaboratory/KOMP_Images_ETL.git

```



<br>
<br>

# Usage

To use, please check the word document contained in this repositiory. 




## Credits
This project use the Jean-Paul Calderone's classic Filesystem of a Python project, for more info, please see the link below:

[Filesystem of a Python project](http://as.ynchrono.us/2007/12/filesystem-structure-of-python-project_21.html)

<br>

The project is created by:

- Tianyu Chen contact:Tianyu.Chen@jax.org
- Michael McFarland contact:Michael.McFarland@jax.org






