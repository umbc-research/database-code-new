#!/usr/home/mb17/database_work/backend/bin/python3

from astropy.io import fits
import pymysql.cursors
import subprocess

def getFiles():
    process = subprocess.Popen(["ls", "/mnt/data"], stdout=subprocess.PIPE)
    output, _ = process.communicate() #unpacks as tuple of stdout and stderr, but we don't care about errors
    output = output.decode("utf-8")
    return output

def returnFitsData(file_location):
    with fits.open(file_location) as hdul:
        hdu = hdul[0]
        fits_data = dict(bitpix = hdu.header["bitpix"], naxis1 = hdu.header["naxis1"], naxis2 = hdu.header["naxis2"], heavenly_object = hdu.header["object"], gain = hdu.header["gain"], camera_filter = hdu.header["filter"], date_obs = hdu.header["date-obs"], frametype = hdu.header["frametyp"], temp = hdu.header["ccd-temp"], ypixsz = hdu.header["ypixsz"], xpixsz = hdu.header["xpixsz"], exptime = hdu.header["exptime"], detector = hdu.header["instrume"])
    return fits_data



def main():
    #hdul = fits.open("/mnt/data/hd163770_11Z_00010.fits")
    file_list = getFiles()
    file_list = file_list.split()
    for i in file_list:
        file_location = "/mnt/data/" + i
        print(file_location)
        fits_data = returnFitsData(file_location)
        print(fits_data["heavenly_object"])
        print(fits_data["exptime"])

if __name__ == "__main__":
    main()
