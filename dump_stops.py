import psycopg2
import os
import sys
import pprint
import json

DB_NAME = "gtfs" # USE CONFIG FILE IN THE FUTURE
USER_NAME = "postgres"
HOST = "169.226.142.154"
PORT = "5432"

PASSWORD = os.environ.get("GTFS_PASSWORD")

conn = psycopg2.connect(database=DB_NAME, user=USER_NAME, password=PASSWORD, host=HOST, port=PORT)
cur = conn.cursor()

geoJson = {
    "type": "FeatureCollection",
    "features": []
}

get_stops_query = "SELECT * FROM cdta_20130906_0131.stops ORDER BY stop_name ASC LIMIT 10;"

cur.execute(get_stops_query)
data = cur.fetchall()

for row in data:
    feature = {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": None
        },
        "properties": {}
    }
    feature["properties"]["stop_id"] = row[0]
    feature["properties"]["stop_name"] = row[2]
    feature["geometry"]["coordinates"] = [float(row[5]), float(row[4])]

    geoJson["features"].append(feature)

print json.dumps(geoJson)


