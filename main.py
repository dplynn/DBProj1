import pymysql 
import csv
import pandas as pd
def mysqlconnect(): 
    # To connect MySQL database 
    conn = pymysql.connect( 
        host='localhost', 
        user='root',  
        password = "shadow916", 
        db='mysql', 
        ) 
    cur = conn.cursor() 
    cur.execute("select @@version") 
    cur.execute(" CREATE DATABASE IF NOT EXISTS testdb")
    cur.execute("USE testdb")
    return cur

def create_tables(cur):
    try: 
        cur.execute('''CREATE TABLE IF NOT EXISTS Player (
    ID CHAR(8) PRIMARY KEY,
    Name VARCHAR(255) NOT NULL,
    Rating FLOAT NOT NULL CHECK (Rating >= 0)
    );''')

        cur.execute('''CREATE TABLE IF NOT EXISTS Game (
    Time DATETIME NOT NULL,
    Acidic CHAR(8) NOT NULL,
    Alkaline CHAR(8) NOT NULL,
    AcScore INT CHECK (AcScore BETWEEN 0 AND 10),
    AkScore INT CHECK (AkScore BETWEEN 0 AND 10),
    AcRating FLOAT CHECK (AcRating >= 0),
    AkRating FLOAT CHECK (AkRating >= 0),
    Tournament VARCHAR(40),
    PRIMARY KEY (Time, Acidic)
        );''')

        cur.execute('''CREATE TABLE IF NOT EXISTS Tournament (
    Name VARCHAR(40) PRIMARY KEY,
    Organizer CHAR(8) NOT NULL
        );''')

        print("Tables created successfully (if not exist)")
    except pymysql.MySQLError as e:
        print(f"Error creating tables: {e}")

def main():
    cur = mysqlconnect()
    cur.execute("SET FOREIGN_KEY_CHECKS = 0;")
    cur.execute("DROP TABLE IF EXISTS Game;")
    cur.execute("DROP TABLE IF EXISTS Player;")
    cur.execute("DROP TABLE IF EXISTS Tournament;")
    cur.execute("SET FOREIGN_KEY_CHECKS = 1;")
    create_tables(cur)
    cur.execute("SHOW TABLES")
    tables = cur.fetchall()
    for table in tables:
        print(table[0])
    file_name = input("Enter CSV file name: ")
    with open(file_name, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            print("Processing command:", row)
            



if __name__ == "__main__" : 
        main()


