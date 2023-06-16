from neo4j import GraphDatabase
import csv


class InsertCSGO:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def insert_player_country(self, player, country):
        with self.driver.session() as session:
            greeting = session.execute_write(self._create_player_country, player, country)
            print(greeting)

    @staticmethod
    def _create_player_country(tx, player, country):
        result = tx.run("MERGE (a:Player {name: $player}) "
                        "MERGE (b:Country {name: $country}) "
                        "MERGE (a)-[r:from]->(b) "
                        "RETURN type(r)", player=player, country=country)
        return result.single()[0]


if __name__ == "__main__":
  
    greeter = InsertCSGO("bolt://localhost:7687", "neo4j", "trabalhobd3")
  
    with open('players.csv', newline='') as csvfile:
      reader = csv.DictReader(csvfile)
      for row in reader:
          player_name = row['player_name']
          country = row['country']
          greeter.insert_player_country(player_name, country)

    greeter.close()