from neo4j import GraphDatabase
import csv


class InsertCSGO:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def insert_map_vetted(self, team, map):
        with self.driver.session() as session:
            session.execute_write(self._create_map_vetted, team, map)

    @staticmethod
    def _create_map_vetted(tx, team, map):
        result = tx.run("MERGE (a:Team {name: $team}) "
                        "MERGE (b:Map {name: $map}) "
                        "MERGE (a)-[r:VETTED]->(b) "
                        "RETURN type(r)", team=team, map=map)
        return result.single()[0]
    

    def insert_player_country(self, player, country):
        with self.driver.session() as session:
            session.execute_write(self._create_player_country, player, country)

    @staticmethod
    def _create_player_country(tx, player, country):
        result = tx.run("MERGE (a:Player {name: $player}) "
                        "MERGE (b:Country {name: $country}) "
                        "MERGE (a)-[r:from]->(b) "
                        "RETURN type(r)", player=player, country=country)
        return result.single()[0]
    
    def insert_match(self, match):
        with self.driver.session() as session:
            session.execute_write(self._insert_match, match)

    @staticmethod
    def _insert_match(tx, match):
        result = tx.run("MERGE (a:Match {id: $match}) RETURN a", match=match)
        return result.single()[0]
    
    def insert_player_match(self, player, match):
        with self.driver.session() as session:
            session.execute_write(self._insert_player_match, player, match)
    
    @staticmethod
    def _insert_player_match(tx, player, match):
        result = tx.run("MATCH (a:Player {name: $player}) "
                        "MATCH (b:Match {id: $match}) "
                        "MERGE (a)-[r:played]->(b) "
                        "RETURN type(r)", player=player, match=match)
        return result.single()[0]


if __name__ == "__main__":
  
    inserter = InsertCSGO("bolt://localhost:7687", "neo4j", "trabalhobd3")
  
    with open('picks.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        print ("Inserting picks...")
        for row in reader:
            team_1 = row['team_1']
            team_2 = row['team_2']
            t1_removed_1 = row['t1_removed_1']
            t2_removed_1 = row['t2_removed_1']
            inserter.insert_map_vetted(team_1, t1_removed_1)
            inserter.insert_map_vetted(team_2, t2_removed_1)
        
        print ("Inserting picks... Done!")

    
    
    with open('players.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        print ("Inserting players...")
        for row in reader:
            player_name = row['player_name']
            country = row['country']
            match = row['match_id']
            inserter.insert_player_country(player_name, country)
            inserter.insert_match(match)
            inserter.insert_player_match(player_name, match)

        print ("Inserting players... Done!")

  
    inserter.close()