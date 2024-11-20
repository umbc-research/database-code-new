#!/usr/home/mb17/database_work/backend/bin/python3

from astropy.io import fits
import pymysql.cursors
import subprocess
import shutil
import get_password
import logging


## Global variables, needed to connect to database.

DATABASE = "test_table"
TABLE = "fits_metadata"
PASSWORD = get_password.main()
HOST = "localhost"
USER = "root"
CHARSET = "utf8mb4"


logging.basicConfig(
        filename="my_log.log",
        filemode="a",
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s'
)

########################################################
# Name: connect
# Purpose: To allow for connection to the DB
# Arguments: N/A
# Return: connection (cursor.DictCursor)
########################################################

def connect():
    connection = pymysql.connect(host=HOST,
                                 user=USER,
                                 password=PASSWORD,
                                 database=DATABASE,
                                 charset=CHARSET,
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection

#/mnt/STS -> short term storage, where files will be placed after sharpcap creates them
#/mnt/LTS -> long term storage, where the files will be moved to and held for future querying 
# yes I know LTS also stands for long term support, but I'm struggling to make clear names here

def getFiles():
    process = subprocess.Popen(["ls", "/mnt/STS"], stdout=subprocess.PIPE)
    output, _ = process.communicate() #unpacks as tuple of stdout and stderr, but we don't care about errors
    output = output.decode("utf-8")
    return output

def checkFiles(file_name):
    if not file_name.endswith(".fits"):
        error = "File: {file} is not a fits file, it not be moved to LTS".format(file=file_name)
        logging.error(error)
        return None
    else:
        return file_name

def moveFiles(file_name):
    curr_file_location = "/mnt/STS/" + file_name
    long_term_file_location = "/mnt/LTS/" + file_name
    shutil.move(curr_file_location, long_term_file_location)
    return long_term_file_location

def returnFitsData(file_location):
    with fits.open(file_location) as hdul:
        hdu = hdul[0]
        fits_data = dict(bitpix = hdu.header.get("bitpix", None),
                         naxis1 = hdu.header.get("naxis1", None),
                         naxis2 = hdu.header.get("naxis2", None),
                         heavenly_object = hdu.header.get("object", None),
                         gain = hdu.header.get("gain", None),
                         camera_filter = hdu.header.get("filter", None),
                         date_obs = hdu.header.get("date-obs", None),
                         frametype = hdu.header.get("frametyp", None),
                         temp = hdu.header.get("ccd-temp", None),
                         ypixsz = hdu.header.get("ypixsz", None),
                         xpixsz = hdu.header.get("xpixsz", None),
                         exptime = hdu.header.get("exptime", None),
                         detector = hdu.header.get("instrume", None))
        for key in fits_data:
            if fits_data[key] is None:
                error = "File: {file} is missing the {missing_key} information. This data will not be ingested".format(file=file_location, missing_key=key)
                logging.error(error)
                return None
            elif isinstance(fits_data[key], str):
                fits_data[key] = fits_data[key].replace(" ","_").upper()
    return fits_data

def ingestion(connection, file_location, fits_data):
    with connection.cursor() as cursor:
        #there's probably a better way to write this because this is horrible.
        insert = """INSERT INTO {table} (path, bitpix, naxis1, naxis2, object, gain, filter, date_obs,
        frametype, ccd_temp, xpixsz, ypixsz, exptime, detector) 
        VALUES ('{path}', '{bitpix}', '{naxis1}', '{naxis2}', '{heavenly_object}', '{gain}', '{camera_filter}', '{date_obs}', '{frametype}', 
        '{temp}', '{ypixsz}', '{xpixsz}', '{exptime}', '{detector}')""".format(
                table=TABLE,path=file_location, bitpix=fits_data["bitpix"], naxis1=fits_data["naxis1"], naxis2=fits_data["naxis2"],
                heavenly_object=fits_data["heavenly_object"], gain=fits_data["gain"], camera_filter=fits_data["camera_filter"],
                date_obs=fits_data["date_obs"], frametype=fits_data["frametype"], temp=fits_data["temp"], ypixsz=fits_data["ypixsz"],
                xpixsz=fits_data["xpixsz"], exptime=fits_data["exptime"], detector=fits_data["detector"]).replace("\n", "").replace("   ","")
        print(insert)
        cursor.execute(insert)
        connection.commit()



def main():
    connection = connect()
    file_list = getFiles()
    file_list = file_list.split()
    for file_name in file_list:
        file_status = checkFiles(file_name)
        if file_status != None:
            temp_file_location = "/mnt/STS/" + file_name
            fits_data = returnFitsData(temp_file_location)
            if fits_data != None:
                file_location = moveFiles(file_name)
                ingestion(connection, file_location, fits_data)


if __name__ == "__main__":
    main()
