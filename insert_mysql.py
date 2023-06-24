import mysql.connector
import pandas as pd


class InsertCSGO:
    def __init__(self, host='127.0.0.1', user='root', password='mysql', database='csgo'):
        self.connection = None
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def establish_connection(self):
        self.connection = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )

    def close(self):
        if self.connection:
            self.connection.close()

    def create_tables(self):
        create_tables_query = """
            CREATE TABLE IF NOT EXISTS teams (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255)
            );            
            CREATE TABLE IF NOT EXISTS maps (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255)
            );
            CREATE TABLE IF NOT EXISTS matches (
                id INT PRIMARY KEY
            );
            CREATE TABLE IF NOT EXISTS vetted (
                id INT AUTO_INCREMENT PRIMARY KEY,
                team_id INT,
                map_id INT,
                match_id INT,
                FOREIGN KEY (team_id) REFERENCES teams(id),
                FOREIGN KEY (map_id) REFERENCES maps(id),
                FOREIGN KEY (match_id) REFERENCES matches(id)
            );
            CREATE TABLE IF NOT EXISTS countries (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255)
            );
            CREATE TABLE IF NOT EXISTS players (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                country_id INT,
                FOREIGN KEY (country_id) REFERENCES countries(id)
            );
            CREATE TABLE IF NOT EXISTS player_match (
                id INT AUTO_INCREMENT PRIMARY KEY,
                player_id INT,
                match_id INT,
                FOREIGN KEY (player_id) REFERENCES players(id),
                FOREIGN KEY (match_id) REFERENCES matches(id)
            );
        """
        cursor = self.connection.cursor()
        cursor.execute(create_tables_query)
        self.close()

    def insert_country(self, country):
        query = "INSERT INTO countries (name) VALUES (%s)"
        cursor = self.connection.cursor()
        cursor.execute(query, (country,))
        self.connection.commit()

    def insert_map(self, map):
        query = "INSERT INTO maps (name) VALUES (%s)"
        self.connection.cursor().execute(query, (map,))
        self.connection.commit()

    def insert_match(self, match_id):
        query = "INSERT INTO matches (id) VALUES (%s)"
        self.connection.cursor().execute(query, (match_id,))
        self.connection.commit()

    def insert_player(self, player_id, player_name, country_name):
        query_country = "SELECT (id) FROM countries WHERE name = %s"
        cursor = self.connection.cursor()
        cursor.execute(query_country, (country_name, ))
        result = cursor.fetchone()
        if result:
            country_id = result[0]
            query_insert_player = "INSERT INTO players (id, name, country_id) VALUES (%s, %s, %s)"
            cursor.execute(query_insert_player, (player_id, player_name, country_id))
            self.connection.commit()

    def insert_team(self, team_name):
        query = "INSERT INTO teams (name) VALUES (%s)"
        self.connection.cursor().execute(query, (team_name,))
        self.connection.commit()

    def insert_player_match(self, player_id, match_id):
        query = "INSERT INTO player_match (player_id, match_id) VALUES (%s, %s)"
        self.connection.cursor().execute(query, (player_id, match_id))
        self.connection.commit()

    def insert_vetted(self, team_id, map_id, match_id):
        query = "INSERT INTO vetted (team_id, map_id, match_id) VALUES (%s, %s, %s)"
        self.connection.cursor().execute(query, (team_id, map_id, match_id))
        self.connection.commit()

    def get_maps(self):
        query = "SELECT * FROM maps"
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def get_teams(self):
        query = "SELECT * FROM teams"
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()

if __name__ == "__main__":
    greeter = InsertCSGO("127.0.0.1", "root", "root", "csgo")

    try:
        greeter.establish_connection()
        greeter.create_tables()

        results = pd.read_csv('data/results.csv')
        picks = pd.read_csv('data/picks.csv')
        players = pd.read_csv('data/players.csv')

        # remove lines with null or empty values
        results = results.dropna()
        picks = picks.dropna()
        players = players.dropna()

        greeter.establish_connection()

        print("Inserting countries...")
        count = 0
        countries = pd.concat([players['country']]).drop_duplicates()
        for country in countries:
            greeter.insert_country(country)
            count += 1
            if count % 100 == 0:
                print("Inserted " + str(count) + " countries")
        print("Inserting countries...Done")

        print("Inserting maps...")
        count = 0
        maps = pd.concat([picks['t1_picked_1']]).drop_duplicates()
        for map in maps:
            greeter.insert_map(map)
            count += 1
            if count % 100 == 0:
                print("Inserted " + str(count) + " maps")
        print("Inserting maps...Done")

        print("Inserting matches...")
        count = 0
        matches_ids = results['match_id'].drop_duplicates()
        for match_id in matches_ids:
            greeter.insert_match(match_id)
            count += 1
            if count % 100 == 0:
                print("Inserted " + str(count) + " matches")
        print("Inserting matches...Done")

        print("Inserting players...")
        players_df = pd.DataFrame(columns=['player_name', 'country'])
        players_df['player_name'] = players['player_name']
        players_df['player_id'] = players['player_id']
        players_df['country'] = players['country']
        players_df = players_df.drop_duplicates(['player_name', 'player_id'])
        count = 0
        for index, row in players_df.iterrows():
            greeter.insert_player(row['player_id'], row['player_name'], row['country'])
            count += 1
            if count % 100 == 0:
                print("Inserted " + str(count) + " players")
        print("Inserting players....Done")

        print("Inserting teams...")
        teams = pd.concat([results['team_1'], results['team_2']]).drop_duplicates()
        count = 0
        for team in teams:
            greeter.insert_team(team)
            count += 1
            if count % 100 == 0:
                print("Inserted " + str(count) + " teams")
        print("Inserting teams...Done")

        print("Inserting player_match...")
        count = 0
        for index, row in players.iterrows():
            player_id, match_id = row['player_id'], row['match_id']
            greeter.insert_player_match(player_id, match_id)
            count += 1
            if count % 100 == 0:
                print("Inserted " + str(count) + " players_match")
        print("Inserting player_match...Done")

        print("Inserting vetted...")
        maps = greeter.get_maps()
        maps_hash = {m[1]: m[0] for m in maps}
        teams = greeter.get_teams()
        teams_hash = {t[1]: t[0] for t in teams}
        count = 0
        for index, row in picks.iterrows():
            match_id = row['match_id']
            team_1, t1_removed_1 = teams_hash[row['team_1']], maps_hash[row['t1_removed_1']]
            team_2, t2_removed_1 = teams_hash[row['team_2']], maps_hash[row['t2_removed_1']]
            greeter.insert_vetted(team_1, t1_removed_1, match_id)
            greeter.insert_vetted(team_2, t2_removed_1, match_id)
            count += 1
            if count % 100 == 0:
                print("Inserted " + str(count) + " vetted")
        print("Inserting vetted...Done")
    except mysql.connector.Error as error:
        print("Erro ao inserir dados:", error)
        greeter.connection.rollback()
    finally:
        greeter.close()
