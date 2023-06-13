from neo4j import GraphDatabase
import csv


class InsertCSGO:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def insert_map_vetted(self, team, map):
        with self.driver.session() as session:
            greeting = session.execute_write(self._create_map_vetted, team, map)
            print(greeting)

    @staticmethod
    def _create_map_vetted(tx, team, map):
        result = tx.run("MERGE (a:Team {name: $team}) "
                        "MERGE (b:Map {name: $map}) "
                        "MERGE (a)-[r:VETTED]->(b) "
                        "RETURN type(r)", team=team, map=map)
        return result.single()[0]


if __name__ == "__main__":
  
    greeter = InsertCSGO("bolt://localhost:7687", "neo4j", "trabalhobd3")
  
    with open('picks.csv', newline='') as csvfile:
      reader = csv.DictReader(csvfile)
      for row in reader:
          team_1 = row['team_1']
          team_2 = row['team_2']
          t1_removed_1 = row['t1_removed_1']
          t2_removed_1 = row['t2_removed_1']
          greeter.insert_map_vetted(team_1, t1_removed_1)
          greeter.insert_map_vetted(team_2, t2_removed_1)

  
    greeter.print_greeting("hello, world")
    greeter.close()