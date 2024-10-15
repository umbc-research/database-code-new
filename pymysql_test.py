#!/usr/home/mb17/database_work/backend/bin/python3

import pymysql.cursors
"""
Assumes the following table has been created already:
    CREATE TABLE `users` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `email` varchar(255) COLLATE utf8_bin NOT NULL,
    `password` varchar(255) COLLATE utf8_bin NOT NULL,
    PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=1 ;
"""

def read_password(path):
    try:
        with open(path, 'r') as file:
            password = file.read().strip()  # Stripping extra spaces/newlines
        return password
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        exit()

def main():
    password = read_password("./.password")
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password=password,
                                 database='obsstor_test',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor) #a cursor is just a object used to interact with the database, this particular cursor returns results as a dictionary
    with connection:
        with connection.cursor() as cursor:
            sql = "INSERT INTO `test` (`email`, `password`) VALUES (%s, %s)"
            cursor.execute(sql, ('webmaster@python.org', 'very-secret'))

        connection.commit()
        with connection.cursor() as cursor:
            sql = "SELECT `id`, `password` FROM `test` WHERE `email`=%s"
            cursor.execute(sql, ('webmaster@python.org',))
            result = cursor.fetchone()
            print(result)

if __name__ == "__main__":
    main()
