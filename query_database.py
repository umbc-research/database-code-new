#!/usr/home/mb17/database_work/backend/bin/python3

#import argparse
import get_password
import read_files
import pymysql.cursors

DATABASE = "obsstor_test"
TABLE = "fits_metadata"
PASSWORD = get_password.main()
HOST = "localhost"
USER = "root"
CHARSET = "utf8mb4"


def connect():
    connection = pymysql.connect(host=HOST,
                                 user=USER,
                                 password=PASSWORD,
                                 database=DATABASE,
                                 charset=CHARSET,
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection

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

def welcome_message(queryList):
    fieldLength = len(queryList)
    print("Welcome to UMBC's Observatory Data Quierier.\n")
    print("Enter the number associated with the field you would like to query")
    fieldNumber = 0
    while fieldNumber < fieldLength:
        print(str(fieldNumber) + ": " + queryList[fieldNumber])
        fieldNumber += 1
    queryField = input()
    return queryField



def main():
    connection = connect()
    queryList = get_fields(connection)
    welcome_message(queryList)

if __name__ == "__main__":
    main()
