import pymysql 
import csv
import pandas as pd
import datetime
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

def handle_command_e(cur):
    create_tables(cur)
    print("Tables checked and created if not present.")

def handle_command_c(cur):
    cur.execute("DELETE FROM Game;")
    cur.execute("DELETE FROM Player;")
    cur.execute("DELETE FROM Tournament;")
    print("All data cleared.")

def handle_command_p(cur, row):
    try:
        cur.execute("INSERT INTO Player (ID, Name, Rating) VALUES (%s, %s, %s)", (row[1], row[2], float(row[3])))
        print(f"Player {row[2]} added.")
    except pymysql.MySQLError as e:
        print(f"Error adding player {row[2]}: {e}")

def handle_command_g(cur, row):
    try:
        # Construct the time string in the correct format
        date_part = row[1]  # Expected format: YYYYMMDD
        time_part = row[2]  # Expected format: HHMM
        time_str = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:]} {time_part[:2]}:{time_part[2:]}:00"

        # Extract data and scores
        acidic_id = row[3]
        alkaline_id = row[4]
        ac_score = int(row[5])
        ak_score = int(row[6])
        ac_rating = float(row[7])
        ak_rating = float(row[8])
        tournament = row[9] if len(row) > 9 else None

        # Insert the game into the Game table
        cur.execute(
            "INSERT INTO Game (Time, Acidic, Alkaline, AcScore, AkScore, AcRating, AkRating, Tournament) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (time_str, acidic_id, alkaline_id, ac_score, ak_score, ac_rating, ak_rating, tournament)
        )

        # Update the ratings of the players in the Player table
        cur.execute("UPDATE Player SET Rating = %s WHERE ID = %s", (ac_rating, acidic_id))
        cur.execute("UPDATE Player SET Rating = %s WHERE ID = %s", (ak_rating, alkaline_id))

        print("Game added and player ratings updated.")
    except pymysql.MySQLError as e:
        print(f"Error adding game: {e}")

def handle_command_r(cur, row):
    try:
        time_str = f"{row[1]} {row[2][:2]}:{row[2][2:]}:00"
        cur.execute("UPDATE Game SET AcScore=%s, AkScore=%s, AcRating=%s, AkRating=%s WHERE Time=%s AND Acidic=%s", (row[7], row[8], row[9], row[10], time_str, row[3]))
        print("Game result updated.")
    except pymysql.MySQLError as e:
        print(f"Error updating game result: {e}")

def handle_command_t(cur, row, reader):
    try:
        cur.execute("INSERT INTO Tournament (Name, Organizer) VALUES (%s, %s)", (row[1], row[2]))
        print(f"Tournament {row[1]} added.")
        for _ in range(int(row[3])):
            game_row = next(reader)
            handle_command_g(cur, game_row)
    except pymysql.MySQLError as e:
        print(f"Error adding tournament: {e}")
def handle_query_p(cur, player_id):
    try:
        cur.execute("""
            SELECT 
                Name, 
                ROUND(Rating, 2), 
                (SELECT COUNT(*) FROM Game WHERE Acidic = %s OR Alkaline = %s) AS GamesPlayed,
                (SELECT COUNT(*) FROM Game WHERE (Acidic = %s AND AcScore > AkScore) OR (Alkaline = %s AND AkScore > AcScore)) AS Wins,
                (SELECT COUNT(*) FROM Game WHERE (Acidic = %s OR Alkaline = %s) AND AcScore = AkScore) AS Ties,
                (SELECT COUNT(*) FROM Game WHERE (Acidic = %s AND AcScore < AkScore) OR (Alkaline = %s AND AkScore < AcScore)) AS Losses
            FROM Player
            WHERE ID = %s
        """, (player_id, player_id, player_id, player_id, player_id, player_id, player_id, player_id, player_id))
        result = cur.fetchone()
        if result:
            print(", ".join(map(str, result)))
        else:
            print("None")
    except pymysql.MySQLError as e:
        print(f"Error querying player: {e}")

def handle_query_t(cur, tournament_name):
    try:
        cur.execute("""
            SELECT 
                DATE(Time) AS GameDate,
                TIME_FORMAT(Time, '%H:%i') AS GameTime,
                (SELECT Name FROM Player WHERE ID = Acidic) AS AcidicName,
                Acidic,
                (SELECT Name FROM Player WHERE ID = Alkaline) AS AlkalineName,
                Alkaline,
                AcScore,
                AkScore
            FROM Game
            WHERE Tournament = %s
            ORDER BY Time
        """, (tournament_name,))
        results = cur.fetchall()
        if results:
            for row in results:
                print(", ".join(map(str, row)))
        else:
            print("None")
    except pymysql.MySQLError as e:
        print(f"Error querying tournament: {e}")

def handle_query_h(cur, player1_id, player2_id):
    try:
        cur.execute("""
            SELECT 
                DATE(Time) AS GameDate,
                TIME_FORMAT(Time, '%H:%i') AS GameTime,
                (SELECT Name FROM Player WHERE ID = Acidic) AS AcidicName,
                Acidic,
                (SELECT Name FROM Player WHERE ID = Alkaline) AS AlkalineName,
                Alkaline,
                AcScore,
                AkScore
            FROM Game
            WHERE (Acidic = %s AND Alkaline = %s) OR (Acidic = %s AND Alkaline = %s)
            ORDER BY Time
        """, (player1_id, player2_id, player2_id, player1_id))
        results = cur.fetchall()
        if results:
            for row in results:
                print(", ".join(map(str, row)))
        else:
            print("None")
    except pymysql.MySQLError as e:
        print(f"Error querying head-to-head games: {e}")

def handle_query_d(cur, start_date, end_date):
    try:
        cur.execute("""
            SELECT 
                Player.ID,
                Player.Name,
                COUNT(Game.Time) AS GamesPlayed,
                SUM(CASE WHEN (Game.Acidic = Player.ID AND Game.AcScore > Game.AkScore) OR (Game.Alkaline = Player.ID AND Game.AkScore > Game.AcScore) THEN 1 ELSE 0 END) AS Wins,
                SUM(CASE WHEN (Game.Acidic = Player.ID OR Game.Alkaline = Player.ID) AND Game.AcScore = Game.AkScore THEN 1 ELSE 0 END) AS Ties,
                SUM(CASE WHEN (Game.Acidic = Player.ID AND Game.AcScore < Game.AkScore) OR (Game.Alkaline = Player.ID AND Game.AkScore < Game.AcScore) THEN 1 ELSE 0 END) AS Losses,
                (2 * SUM(CASE WHEN (Game.Acidic = Player.ID AND Game.AcScore > Game.AkScore) OR (Game.Alkaline = Player.ID AND Game.AkScore > Game.AcScore) THEN 1 ELSE 0 END) +
                 SUM(CASE WHEN (Game.Acidic = Player.ID OR Game.Alkaline = Player.ID) AND Game.AcScore = Game.AkScore THEN 1 ELSE 0 END)) AS Points
            FROM Player
            LEFT JOIN Game ON Player.ID = Game.Acidic OR Player.ID = Game.Alkaline
            WHERE DATE(Game.Time) BETWEEN %s AND %s
            GROUP BY Player.ID
            ORDER BY Points DESC
        """, (start_date, end_date))
        results = cur.fetchall()
        if results:
            for row in results:
                print(", ".join(map(str, row)))
        else:
            print("None")
    except pymysql.MySQLError as e:
        print(f"Error querying player rankings: {e}")

def main():
    cur = mysqlconnect()
    create_tables(cur)

    file_name = input("Enter CSV file name: ")
    with open(file_name, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            command = row[0]
            if command == 'e':
                handle_command_e(cur)
            elif command == 'c':
                handle_command_c(cur)
            elif command == 'p':
                handle_command_p(cur, row)
            elif command == 'g':
                handle_command_g(cur, row)
            elif command == 'r':
                handle_command_r(cur, row)
            elif command == 't':
                handle_command_t(cur, row, reader)
            elif command == 'P':
                handle_query_p(cur, row[1])
            elif command == 'T':
                handle_query_t(cur, row[1])
            elif command == 'H':
                handle_query_h(cur, row[1], row[2])
            elif command == 'D':
                handle_query_d(cur, row[1], row[2])
            else:
                print(f"Unknown command: {command}")
    cur.close()

if __name__ == "__main__": 
    main()


