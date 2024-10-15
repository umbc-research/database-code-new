#!/usr/home/mb17/database_work/backend/bin/python3

#######################################
## Name: query_database.py 
## Purpose: To provide a reasonably easy
## program to run to query observatory data
## Author: Max Breitmeyer
## Date: 20241015
## Arguments: N/A
#######################################

########################################################
# Function:
# Purpose:
# Arguments:
# Return:
########################################################

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
# Return: queryBy (list of tuples, (Field, Type))
########################################################

def get_fields(connection):
    queryBy = []
    with connection:
        with connection.cursor() as cursor:
            sql = "DESCRIBE " + TABLE
            cursor.execute(sql)
            indexList = cursor.fetchall()
            print(indexList)
            for indexDict in indexList:
                #print(indexDict["Field"])
                queryBy.append((indexDict["Field"], indexDict["Type"]))
            return queryBy

########################################################
# Name: map_query_list
# Purpose: helper function to map list of querieable items to numbers
# Arguments: queryList (List);
# Return: fieldDict (dictionary)
########################################################

def map_query_list(queryList):
    fieldNumber = 0
    fieldLength = len(queryList)
    fieldDict = {}
    while fieldNumber < fieldLength:
        fieldDict[str(fieldNumber)] = queryList[fieldNumber]
        fieldNumber += 1
    return fieldDict

########################################################
# Function: welcome_message
# Purpose: Gives a welcome message to users, and displays options to query the table
# Arguments: fieldDict (Dict); queriable fields from DB
# Return: queried (List); list of fields the user want to query
########################################################

def welcome_message(fieldDict):
    print("Welcome to UMBC's Observatory Data Quierier.\n")
    print("Enter the number associated with the field you would like to query (if you would like to query more than one field enter multiple numbers separated by commas):")
    for key in fieldDict:
        print(key + ": " + fieldDict[key][0])
    queryFieldsInput = input()
    queried = queryFieldsInput.split(",")
    return queried

########################################################
# Function: get_queries
# Purpose: Takes the list of queried rows, and asks the data that is actually queried
# Arguments: queried (list)
# Return: queryDict (Dict; "field": "query_to_be_made")
########################################################

def get_queries(queried, fieldDict):
    queryDict = {}
    print("You are trying to query the following: ")
    for key in queried:
        field = fieldDict[key][0]
        fieldType = fieldDict[key][1]
        print(field + " has the type: " + fieldType)
        result = input("Enter a query with the matching data type (for date enter as YYYY-MM-DD): ")
        queryDict[field] = result
    return queryDict


########################################################
# Function: sanitize_query
# Purpose: Takes users query inputs and sanitizes it into one string for the actual query
# Arguments: queryDict(dictionary; ("field": "query_to_be_made")
# Return: query (string; a sanitized query for sql)
########################################################

def sanitize_query(queryDict):
    query = "SELECT * FROM {table} WHERE".format(table=TABLE)
    print(query)

def main():
    connection = connect()
    queryList = get_fields(connection)
    fieldDict = map_query_list(queryList)
    queried = welcome_message(fieldDict)
    queryDict = get_queries(queried, fieldDict)
    sanitize_query(queryDict)

if __name__ == "__main__":
    main()
