from neo4j import GraphDatabase
import pandas as pd


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
        result = tx.run("MATCH (a:Team {name: $team}) "
                        "MATCH (b:Map {name: $map}) "
                        "MERGE (a)-[r:VETTED]->(b) "
                        "RETURN type(r)", team=team, map=map)
        return result.single()[0]
    

    def insert_player_country(self, player, country):
        with self.driver.session() as session:
            session.execute_write(self._create_player_country, player, country)

    @staticmethod
    def _create_player_country(tx, player, country):
        result = tx.run("MATCH (b:Country {name: $country}) "
                        "MERGE (a:Player {name: $player}) "
                        "MERGE (a)-[r:FROM]->(b) "
                        "RETURN type(r)", player=player, country=country)
        return result.single()[0]

    def insert_country(self, country):
        with self.driver.session() as session:
            session.execute_write(self._create_country, country)

    @staticmethod
    def _create_country(tx, country):
        result = tx.run("MERGE (a:Country {name: $country}) "
                        "RETURN a", country=country)
        return result.single()[0]

    def insert_match(self, match):
        with self.driver.session() as session:
            session.execute_write(self._insert_match, match)

    @staticmethod
    def _insert_match(tx, match):
        result = tx.run("MERGE (a:Match {id: $match}) RETURN a", match=match)
        return result.single()[0]
    

    def insert_team(self, team):
        with self.driver.session() as session:
            session.execute_write(self._insert_team, team)

    @staticmethod
    def _insert_team(tx, team):
        result = tx.run("MERGE (a:Team {name: $team}) RETURN a", team=team)
        return result.single()[0]

    def insert_player_match(self, player, match):
        with self.driver.session() as session:
            session.execute_write(self._insert_player_match, player, match)
    
    @staticmethod
    def _insert_player_match(tx, player, match):
        result = tx.run("MATCH (a:Player {name: $player}) "
                        "MATCH (b:Match {id: $match}) "
                        "MERGE (a)-[r:PLAYER]->(b) "
                        "RETURN type(r)", player=player, match=match)
        return result.single()[0]
    
    def insert_team_match(self, team, match):
        with self.driver.session() as session:
            session.execute_write(self._insert_team_match, team, match)

    @staticmethod
    def _insert_team_match(tx, team, match):
        result = tx.run("MATCH (a:Team {name: $team}) "
                        "MATCH (b:Match {id: $match}) "
                        "MERGE (a)-[r:DISPUTED]->(b) "
                        "RETURN type(r)", team=team, match=match)
        return result.single()[0]

    def insert_winner_match(self, team, match):
        with self.driver.session() as session:
            session.execute_write(self._insert_winner_match, team, match)

    @staticmethod
    def _insert_winner_match(tx, team, match):
        result = tx.run("MATCH (a:Team {name: $team}) "
                        "MATCH (b:Match {id: $match}) "
                        "MERGE (a)-[r:DISPUTE_WINNER]->(b) "
                        "RETURN type(r)", team=team, match=match)
        return result.single()[0]

    def run_query(self, query):
        return self.driver.execute_query(query)

if __name__ == "__main__":
  
    inserter = InsertCSGO("bolt://localhost:7687", "neo4j", "trabalhobd3")
  
    results = pd.read_csv('data/results.csv')
    picks = pd.read_csv('data/picks.csv')
    players = pd.read_csv('data/players.csv')

    # remove lines with null or empty values
    results = results.dropna()
    picks = picks.dropna()
    players = players.dropna()

    print ("Inserting teams...")
    teams = pd.concat([results['team_1'], results['team_2']]).drop_duplicates()
    count = 0
    for team in teams:
        inserter.insert_team(team)
        count += 1
        if count % 100 == 0:
            print("Inserted " + str(count) + " teams")
    print ("Inserting teams... Done!")

    print("Inserting countries...")
    countries = pd.concat([players['country']]).drop_duplicates()
    for country in countries:
        inserter.insert_country(country)
        count += 1
        if count % 100 == 0:
            print("Inserted " + str(count) + " countries")
    print("Inserting countries... Done!")

    print ("Inserting players...")
    players_df = pd.DataFrame(columns=['player_name', 'country'])
    players_df['player_name'] = players['player_name']
    players_df['country'] = players['country']
    players_df = players_df.drop_duplicates(['player_name'])
    count = 0
    for index, row in players_df.iterrows():
        inserter.insert_player_country(row['player_name'], row['country'])
        count += 1
        if count % 100 == 0:
            print("Inserted " + str(count) + " players")
    print ("Inserting players... Done!")

    print ("Inserting matches...")
    count = 0
    matches_ids = results['match_id'].drop_duplicates()
    for match_id in matches_ids:
        inserter.insert_match(match_id)
        count += 1
        if count % 100 == 0:
            print("Inserted " + str(count) + " matches")
    print ("Inserting matches... Done!")

    print ("Inserting players matches...")
    count = 0
    for index, row in players.iterrows():
        inserter.insert_player_match(row['player_name'], row['match_id'])
        count += 1
        if count % 100 == 0:
            print("Inserted " + str(count) + " players matches")
    print ("Inserting players matches... Done!")

    print ("Inserting results...")
    count = 0
    for index, row in results.iterrows():
        match_id = row['match_id']
        winner_team = row['team_1'] if row['result_1'] > row['result_2'] else row['team_2']
        inserter.insert_team_match(row['team_1'], match_id)
        inserter.insert_team_match(row['team_2'], match_id)
        inserter.insert_winner_match(winner_team, match_id)
        count += 1
        if count % 100 == 0:
            print("Inserted " + str(count) + " results")
    print ("Inserting results... Done!")

    print ("Inserting picks...")
    count = 0
    for index, row in picks.iterrows():
        team_1 = row['team_1']
        team_2 = row['team_2']
        t1_removed_1 = row['t1_removed_1']
        t2_removed_1 = row['t2_removed_1']
        inserter.insert_map_vetted(team_1, t1_removed_1)
        inserter.insert_map_vetted(team_2, t2_removed_1) 
        count += 1
        if count % 100 == 0:
            print("Inserted " + str(count) + " picks")
    print ("Inserting picks... Done!")


    inserter.close()