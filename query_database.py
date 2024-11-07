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
# Name:
# Purpose:
# Arguments:
# Return:
########################################################

import get_password
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
    with connection.cursor() as cursor:
        sql = "DESCRIBE " + TABLE
        cursor.execute(sql)
        indexList = cursor.fetchall()
        print(indexList)
        for indexDict in indexList:
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
# Name: welcome_message
# Purpose: Gives a welcome message to users, and displays options to query the table
# Arguments: fieldDict (Dict); queriable fields from DB
# Return: queried (List); list of fields the user want to query
########################################################

def welcome_message(fieldDict):
    print("Welcome to UMBC's Observatory Data Quierier.\n")
    print("Enter the number associated with the field you would like to query (if you would like to query more than one field enter multiple numbers separated by commas), or n to exit:")
    for key in fieldDict:
        print(key + ": " + fieldDict[key][0])
    queryFieldsInput = input()
    length = len(queryFieldsInput)
    if queryFieldsInput.lower() == "n":
        print("Goodbye")
        exit()
    
    #some "error handling"
    if queryFieldsInput == "":
        while true: 
        queryFieldsInput = input("You've entered an empty field, please enter a list of numbers for the fields you want to query")
    if queryFieldsInput[length-1] == ",":
        while true:
            badList  = input("Your list ended with a \",\". Did you mean add another item to query? (y/n) ")
            if badList.lower() == "y":
                newInput = input("Add more fields to query: ")
                queryFieldsInput += newInput
                break
            elif badList.lower() == "n":
                queryFieldsInput = queryFieldsInput[0:length - 2]
                break
            else:
                continue



    queried = queryFieldsInput.split(",")
    return queried

########################################################
# Name: get_queries
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
# Name: sanitize_query
# Purpose: Takes users query inputs and sanitizes it into one string for the actual query
# Note*: Sanitize typically has a specific meaning with regards to queries, and making sure users can't commit sql injection...
# ... Since I never allow for users to enter directly to the table, I need to worry about it less. 
# Arguments: queryDict(dictionary; ("field": "query_to_be_made")
# Return: query (string; a sanitized query for sql)
########################################################

def sanitize_query(queryDict):
    query = "SELECT * FROM {table} WHERE ".format(table=TABLE)
    length = len(queryDict)
    for key in queryDict:
        if length != 1: #technically this could be a problem if the user submitted 0 inputs, but the program fails before it gets to here, so I chose to leave it.
            query += "'{field}'='{query}' AND ".format(field=key, query=queryDict[key])
            length -=1
        else:
            query += "'{field}'='{query}'".format(field=key, query=queryDict[key])
    print(query)
    return query

########################################################
# Name: execute_query 
# Purpose: executes the query that was given in sanitize_query
# Arguments: query (string), connection (cursorDict)
# Return: result (Dict from cursorDict)
########################################################

def execute_query(query, connection):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            indexList = cursor.fetchall()
            print (indexList)


def main():
    connection = connect()
    queryList = get_fields(connection)
    fieldDict = map_query_list(queryList)
    queried = welcome_message(fieldDict)
    queryDict = get_queries(queried, fieldDict)
    query = sanitize_query(queryDict)
    execute_query(query, connection)

if __name__ == "__main__":
    main()
