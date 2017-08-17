import psycopg2
import os
import sys
import pprint
import json

DB_NAME = "gtfs" # USE CONFIG FILE IN THE FUTURE
USER_NAME = "postgres"
HOST = "169.226.142.154"
PORT = "5432"

if len(sys.argv) != 2:
    print "Format: python stops_to_geoJSON.py [schema name]"
    sys.exit()

if os.environ.get("GTFS_PASSWORD") is None:
    print "No system variables (GTFS_PASSWORD) set for password"
    sys.exit()

PASSWORD = os.environ.get("GTFS_PASSWORD")
schema_name = sys.argv[1]

conn = psycopg2.connect(database=DB_NAME, user=USER_NAME, password=PASSWORD, host=HOST, port=PORT)
cur = conn.cursor()

gj = {
    "type": "FeatureCollection",
    "features": []
}

query = "SELECT ST_AsGeoJSON(geom) as shape, stop_sequence FROM {0}.stop_times, {0}.stops WHERE stop_times.stop_id = stops.stop_id and trip_id='2304745-AUG13-Troy-Weekday-01' ORDER BY stop_sequence".format(schema_name)
# print query
# [('{"type":"Point","coordinates":[-73.69022,42.73191]}', 1),
cur.execute(query)
data = cur.fetchall()
# pprint.pprint(data)

for r in data:
    gj["features"].append({
        "type": "Feature",
        "geometry": json.loads(r[0]),
        "properties":  {
            "stop_sequence": r[1] # add stop_id l8r?
        }
    })

print json.dumps(gj)

conn.commit()
cur.close()
conn.close()
