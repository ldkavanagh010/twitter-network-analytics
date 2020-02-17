from neo4j import GraphDatabase
import yaml
import sys
import os

with open('config.yml', 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

url = sys.argv[1]
driver = GraphDatabase.driver(cfg['neo4j']['host'], auth=(cfg['neo4j']['user'], cfg['neo4j']['password']))
db = driver.session()
db.run("""USING PERIODIC COMMIT 10000
		  LOAD CSV FROM $url as line
		  MERGE (n:User {id: line[0]})
		  WITH n, line
		  MERGE (m:User {id: line[1]})
		  WITH n, m, line
		  MERGE (n)-[:retweeted {count: line[2], favorites: line[3], retweets: line[4]}]->(m);""", url = url)


