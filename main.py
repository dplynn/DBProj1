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
    return conn, cur

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

def handle_command_p(conn, cur, row):
    try:
        # Validate the rating
        rating = float(row[3])
        if rating < 0:
            print(f"{','.join(row)} Input Invalid")
            return

        # Insert the player into the database
        cur.execute("INSERT INTO Player (ID, Name, Rating) VALUES (%s, %s, %s)", (row[1], row[2], rating))
        conn.commit()
    except pymysql.MySQLError as e:
        print(f"{','.join(row)} Input Invalid")
        conn.rollback()

def handle_command_g(conn, cur, row, tournament=None):
    try:
        # Construct the time string in the correct format
        date_part = row[1]  # Expected format: YYYYMMDD
        time_part = row[2]  # Expected format: HHMM
        time_str = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:]} {time_part[:2]}:{time_part[2:]}:00"

        # Extract player IDs
        acidic_id = row[3]
        alkaline_id = row[4]

        # Validate that both players exist
        cur.execute("SELECT COUNT(*) FROM Player WHERE ID = %s", (acidic_id,))
        if cur.fetchone()[0] == 0:
            print(f"{','.join(row)} Input Invalid")
            return
        cur.execute("SELECT COUNT(*) FROM Player WHERE ID = %s", (alkaline_id,))
        if cur.fetchone()[0] == 0:
            print(f"{','.join(row)} Input Invalid")
            return

        # Check if there are earlier games without results for either player
        if check_earlier_games_without_results(cur, acidic_id, time_str) or check_earlier_games_without_results(cur, alkaline_id, time_str):
            print(f"{','.join(row)} Input Invalid")
            return

        # Check if results are provided
        has_results = len(row) > 8

        if has_results:
            # Validate scores
            ac_score = int(row[5])
            ak_score = int(row[6])
            if not (0 <= ac_score <= 10 and 0 <= ak_score <= 10):
                print(f"{','.join(row)} Input Invalid")
                return

            # Validate ratings
            ac_rating = float(row[7])
            ak_rating = float(row[8])
            if ac_rating < 0 or ak_rating < 0:
                print(f"{','.join(row)} Input Invalid")
                return

            # Insert the game with results
            cur.execute(
                "INSERT INTO Game (Time, Acidic, Alkaline, AcScore, AkScore, AcRating, AkRating, Tournament) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (time_str, acidic_id, alkaline_id, ac_score, ak_score, ac_rating, ak_rating, tournament)
            )
        else:
            # Game without results
            cur.execute(
                "INSERT INTO Game (Time, Acidic, Alkaline, Tournament) "
                "VALUES (%s, %s, %s, %s)",
                (time_str, acidic_id, alkaline_id, tournament)
            )

        conn.commit()
    except pymysql.MySQLError as e:
        print(f"{','.join(row)} Input Invalid")
        conn.rollback()
        
def check_earlier_games_without_results(cur, player_id, game_time):
    """Check if a player has any earlier games without results."""
    cur.execute("""
        SELECT COUNT(*) FROM Game 
        WHERE (Acidic = %s OR Alkaline = %s) 
        AND Time < %s 
        AND AcScore IS NULL
    """, (player_id, player_id, game_time))
    count = cur.fetchone()[0]
    return count > 0

def handle_command_r(conn, cur, row):
    try:
        # Construct the time string in the correct format
        date_part = row[1]  # Expected format: YYYYMMDD
        time_part = row[2]  # Expected format: HHMM
        time_str = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:]} {time_part[:2]}:{time_part[2:]}:00"
        
        acidic_id = row[3]
        alkaline_id = row[4]
        
        # Check if there are earlier games without results for either player
        if check_earlier_games_without_results(cur, acidic_id, time_str) or check_earlier_games_without_results(cur, alkaline_id, time_str):
            print(f"{','.join(row)} Input Invalid")
            return
        
        # Check if the game exists
        cur.execute("SELECT * FROM Game WHERE Time=%s AND Acidic=%s", (time_str, acidic_id))
        game = cur.fetchone()
        
        if not game:
            print(f"{','.join(row)} Input Invalid")
            return
            
        # Update the game with results
        ac_score = int(row[5])
        ak_score = int(row[6])
        ac_rating = float(row[7])
        ak_rating = float(row[8])
        
        cur.execute("""
            UPDATE Game 
            SET AcScore=%s, AkScore=%s, AcRating=%s, AkRating=%s 
            WHERE Time=%s AND Acidic=%s
        """, (ac_score, ak_score, ac_rating, ak_rating, time_str, acidic_id))
        
        # Update player ratings
        cur.execute("UPDATE Player SET Rating = %s WHERE ID = %s", (ac_rating, acidic_id))
        cur.execute("UPDATE Player SET Rating = %s WHERE ID = %s", (ak_rating, alkaline_id))
        
        conn.commit()
    except pymysql.MySQLError as e:
        print(f"{','.join(row)} Input Invalid")
        conn.rollback()

def handle_command_t(conn, cur, row, reader):
    try:
        tournament_name = row[1]
        organizer_id = row[2]
        num_games = int(row[3])
        
        # Insert the tournament
        cur.execute("INSERT INTO Tournament (Name, Organizer) VALUES (%s, %s)", (tournament_name, organizer_id))
        conn.commit()
        print(f"Tournament {tournament_name} added.")
        
        # Process the subsequent game commands
        games_processed = 0
        while games_processed < num_games:
            game_row = next(reader)
            command = game_row[0]
            
            if command == 'g':
                handle_command_g(conn, cur, game_row, tournament_name)
                games_processed += 1
            elif command in ('e', 'c', 't'):
                # Ignore these commands until all tournament games are processed
                pass
            else:
                # Process other commands as normal
                process_command(conn, cur, command, game_row, reader)
                
    except pymysql.MySQLError as e:
        print(f"{','.join(row)} Input Invalid")
        conn.rollback()

def handle_query_p(cur, player_id):
    try:
        cur.execute("""
            SELECT 
                Name, 
                ROUND(Rating, 2) AS Rating, 
                COALESCE((SELECT COUNT(*) FROM Game WHERE (Acidic = %s OR Alkaline = %s) AND AcScore IS NOT NULL), 0) AS GamesPlayed,
                COALESCE((SELECT COUNT(*) FROM Game WHERE (Acidic = %s AND AcScore > AkScore) OR (Alkaline = %s AND AkScore > AcScore) AND AcScore IS NOT NULL), 0) AS Wins,
                COALESCE((SELECT COUNT(*) FROM Game WHERE (Acidic = %s OR Alkaline = %s) AND AcScore = AkScore AND AcScore IS NOT NULL), 0) AS Ties,
                COALESCE((SELECT COUNT(*) FROM Game WHERE (Acidic = %s AND AcScore < AkScore) OR (Alkaline = %s AND AkScore < AcScore) AND AcScore IS NOT NULL), 0) AS Losses
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
                DATE_FORMAT(Time, '%%Y/%%m/%%d') AS GameDate,
                DATE_FORMAT(Time, '%%H:%%i') AS GameTime,
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
                # Format the output to handle NULL values
                formatted_row = []
                for item in row:
                    formatted_row.append(str(item) if item is not None else "NULL")
                print(", ".join(formatted_row))
        else:
            print("None")
    except pymysql.MySQLError as e:
        print(f"Error querying tournament: {e}")

def handle_query_h(cur, player1_id, player2_id):
    try:
        cur.execute("""
            SELECT 
                DATE_FORMAT(Time, '%%Y/%%m/%%d') AS GameDate,
                DATE_FORMAT(Time, '%%H:%%i') AS GameTime,
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
                # Format the output to handle NULL values
                formatted_row = []
                for item in row:
                    formatted_row.append(str(item) if item is not None else "NULL")
                print(", ".join(formatted_row))
        else:
            print("None")
    except pymysql.MySQLError as e:
        print(f"Error querying head-to-head games: {e}")

def handle_query_d(cur, start_date, end_date):
    try:
        # Format dates properly
        start_date = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
        end_date = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"
        
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
            JOIN Game ON (Player.ID = Game.Acidic OR Player.ID = Game.Alkaline) AND Game.AcScore IS NOT NULL
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

def process_command(conn, cur, command, row, reader):
    if command == 'e':
        handle_command_e(cur)
    elif command == 'c':
        handle_command_c(cur)
    elif command == 'p':
        handle_command_p(conn, cur, row)
    elif command == 'g':
        handle_command_g(conn, cur, row)
    elif command == 'r':
        handle_command_r(conn, cur, row)
    elif command == 't':
        handle_command_t(conn, cur, row, reader)
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

def main():
    conn, cur = mysqlconnect()
    create_tables(cur)

    file_name = input("Enter CSV file name: ")
    with open(file_name, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            command = row[0]
            process_command(conn, cur, command, row, reader)
    
    cur.close()
    conn.close()

if __name__ == "__main__": 
    main()