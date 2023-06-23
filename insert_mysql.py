import mysql.connector
import csv


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
            CREATE TABLE IF NOT EXISTS vetted (
                id INT AUTO_INCREMENT PRIMARY KEY,
                team_id INT,
                map_id INT,
                FOREIGN KEY (team_id) REFERENCES teams(id),
                FOREIGN KEY (map_id) REFERENCES maps(id)
            );
            CREATE TABLE IF NOT EXISTS countries (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255)
            );
            CREATE TABLE IF NOT EXISTS players (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                country_id INT,
                FOREIGN KEY (country_id) REFERENCES countries(id)
            );
            CREATE TABLE IF NOT EXISTS matches (
                id INT PRIMARY KEY
            );
            CREATE TABLE IF NOT EXISTS player_match (
                id INT AUTO_INCREMENT PRIMARY KEY,
                player_id INT,
                match_id INT,
                FOREIGN KEY (player_id) REFERENCES players(id),
                FOREIGN KEY (match_id) REFERENCES matches(id)
            );
        """

        self.establish_connection()
        cursor = self.connection.cursor()
        cursor.execute(create_tables_query)
        self.close()

    def insert_map_vetted(self, team, map):
        cursor = self.connection.cursor()
        try:
            # Verificar se o time já existe
            query = "SELECT id FROM teams WHERE name = %s"
            cursor.execute(query, (team,))
            result = cursor.fetchone()
            if result:
                team_id = result[0]
            else:
                # Inserir o novo time
                query = "INSERT INTO teams (name) VALUES (%s)"
                cursor.execute(query, (team,))
                team_id = cursor.lastrowid

            # Verificar se o mapa já existe
            query = "SELECT id FROM maps WHERE name = %s"
            cursor.execute(query, (map,))
            result = cursor.fetchone()
            if result:
                map_id = result[0]
            else:
                # Inserir o novo mapa
                query = "INSERT INTO maps (name) VALUES (%s)"
                cursor.execute(query, (map,))
                map_id = cursor.lastrowid

            # Inserir o relacionamento entre time e mapa
            query = "INSERT INTO vetted (team_id, map_id) VALUES (%s, %s)"
            cursor.execute(query, (team_id, map_id))

            self.connection.commit()
        except mysql.connector.Error as error:
            print("Erro ao inserir dados:", error)
            self.connection.rollback()
        finally:
            cursor.close()

    def insert_player_country(self, player, country):
        cursor = self.connection.cursor()
        try:
            # Verificar se o país já existe
            query = "SELECT id FROM countries WHERE name = %s"
            cursor.execute(query, (country, ))
            result = cursor.fetchone()
            if result:
                country_id = result[0]
            else:
                # Inserir o novo país
                query = "INSERT INTO countries (name) VALUES (%s)"
                cursor.execute(query, (country, ))
                country_id = cursor.lastrowid

            # Verificar se o jogador já existe
            query = "SELECT id FROM players WHERE name = %s"
            cursor.execute(query, (player,))
            result = cursor.fetchone()
            if not result:
                # Inserir o novo jogador
                query = "INSERT INTO players (name, country_id) VALUES (%s, %s)"
                cursor.execute(query, (player, country_id))

            self.connection.commit()
        except mysql.connector.Error as error:
            print("Erro ao inserir dados:", error)
            self.connection.rollback()
        finally:
            cursor.close()

    def insert_player_match(self, player, match):
        cursor = self.connection.cursor()
        try:
            # Verificar se o jogador existe
            query = "SELECT id FROM players WHERE name = %s"
            cursor.execute(query, (player,))
            result = cursor.fetchone()
            if result:
                player_id = result[0]
            else:
                # Caso o jogador não exista
                print("Jogador não encontrado:", player)
                return

            # Verificar se o jogo existe
            query = "SELECT id FROM matches WHERE id = %s"
            cursor.execute(query, (match,))
            result = cursor.fetchone()
            if result:
                match_id = result[0]
            else:
                # Inserir o novo jogo
                query = "INSERT INTO matches (id) VALUES (%s)"
                cursor.execute(query, (match, ))
                match_id = match

            # Inserir o relacionamento entre jogador e jogo
            # Verificar se o relacionamento já existe
            query = "SELECT COUNT(*) FROM player_match WHERE player_id = %s AND match_id = %s"
            cursor.execute(query, (player_id, match_id))
            result = cursor.fetchone()
            if result[0] == 0:
                # O relacionamento não existe, então ocorre a inserção
                query = "INSERT INTO player_match (player_id, match_id) VALUES (%s, %s)"
                cursor.execute(query, (player_id, match_id))

            self.connection.commit()
        except mysql.connector.Error as error:
            print("Erro ao inserir dados:", error)
            self.connection.rollback()
        finally:
            cursor.close()

    def import_csv(self, file_paths):
        self.establish_connection()
        insert_count = 0
        players_set = set()
        vetted_set = set()
        match_set = set()
        for file_path in file_paths:
            with open(file_path, newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                if file_path == "data/picks.csv":
                    for row in reader:
                        team_1 = row['team_1']
                        team_2 = row['team_2']
                        t1_removed_1 = row['t1_removed_1']
                        t2_removed_1 = row['t2_removed_1']
                        vetted_set.add((team_1, t1_removed_1))
                        vetted_set.add((team_2, t2_removed_1))

                elif file_path == "data/players.csv":
                    for row in reader:
                        player = row['player_name']
                        country = row['country']
                        match = row['match_id']
                        players_set.add((player, country))
                        match_set.add((player, match))

        # Inserir jogadores únicos na tabela player_country
        for player, country in players_set:
            insert_count += 1
            if insert_count % 100 == 0:
                print("Inserções realizadas player_country:", insert_count)
            self.insert_player_country(player, country)

        insert_count = 0

        # Inserir combinações únicas de times e mapas na tabela vetted
        for team, map in vetted_set:
            insert_count += 1
            if insert_count % 100 == 0:
                print("Inserções realizadas vetted:", insert_count)
            self.insert_map_vetted(team, map)

        insert_count = 0

        for player, match in match_set:
            insert_count += 1
            if insert_count % 100 == 0:
                print("Inserções realizadas player_match:", insert_count)
            self.insert_player_match(player, match)

        self.close()


if __name__ == "__main__":
    greeter = InsertCSGO("127.0.0.1", "root", "root", "csgo")

    greeter.establish_connection()
    greeter.create_tables()
    greeter.import_csv(["data/picks.csv", "data/players.csv", "data/results.csv"])
    greeter.close()
