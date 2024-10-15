#!/usr/home/mb17/database_work/backend/bin/python3

#######################################
## Name: query_database.py 
## Purpose: To provide a reasonably easy
## program to run to query observatory data
## Author: Max Breitmeyer
## Date: 20241015
## Arguments: N/A
#######################################


import get_password
import read_files
import pymysql.cursors


## Global variables, needed to connect to database. 

DATABASE = "obsstor_test"
TABLE = "fits_metadata"
PASSWORD = get_password.main()
HOST = "localhost"
USER = "root"
CHARSET = "utf8mb4"

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
########################################################
# Name: get_fields
# Purpose: Get all fields that are queriable from the DB Table
# Arguments: connection (DictCursor);
# Return: queryBy (list)
########################################################

def get_fields(connection):
    queryBy = []
    with connection:
        with connection.cursor() as cursor:
            sql = "DESCRIBE " + TABLE
            cursor.execute(sql)
            indexList = cursor.fetchall()
            for indexDict in indexList:
                #print(indexDict["Field"])
                queryBy.append(indexDict["Field"])
            return queryBy

########################################################
# Name: map_QueryList
# Purpose: helper function to map list of querieable items to numbers
# Arguments: queryList (List);
# Return: fieldDict (dictionary)
########################################################

def map_QueryList(queryList):
    fieldNumber = 0
    fieldLength = len(queryList)
    fieldDict = {}
    while fieldNumber < fieldLength:
        fieldDict[fieldNumber] = queryList[fieldNumber]
        fieldNumber += 1
    return fieldDict

########################################################
# Function: welcome_message
# Purpose: Gives a welcome message to users, and displays options to query the table
# Arguments: fieldDict (List); queriable fields from DB
# Return: queried (List); list of fields the user want to query
########################################################

def welcome_message(queryDict):
    print("Welcome to UMBC's Observatory Data Quierier.\n")
    print("Enter the number associated with the field you would like to query (if you would like to query more than one field enter multiple numbers separated by commas):")
    for key in queryDict:
        print(str(key) + ": " + queryDict[key])
    queryFieldsInput = input()
    queried = queryFieldsInput.split(",")
    return queried


def main():
    connection = connect()
    queryList = get_fields(connection)
    fieldDict = map_QueryList(queryList)
    queried = welcome_message(fieldDict)

if __name__ == "__main__":
    main()
