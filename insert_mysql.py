import mysql.connector
import csv

class InsertCSGO:
    def __init__(self, host='127.0.0.1', user='root', password='root', database='csgo'):
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

    def import_csv(self, file_path):
        self.establish_connection()
        with open(file_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                team_1 = row['team_1']
                team_2 = row['team_2']
                t1_removed_1 = row['t1_removed_1']
                t2_removed_1 = row['t2_removed_1']
                self.insert_map_vetted(team_1, t1_removed_1)
                self.insert_map_vetted(team_2, t2_removed_1)
        self.close()


if __name__ == "__main__":
    greeter = InsertCSGO("127.0.0.1", "root", "root", "csgo")

    # Criar as tabelas
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
    """

    greeter.establish_connection()
    cursor = greeter.connection.cursor()
    cursor.execute(create_tables_query)
    cursor.close()

    # Importar dados do arquivo CSV
    greeter.import_csv("data/picks.csv")
    greeter.close()
    

