import psycopg2
import os
import sys
import pprint

DB_NAME = "gtfs" # USE CONFIG FILE IN THE FUTURE
USER_NAME = "postgres"
HOST = "169.226.142.154"
PORT = "5432"

if len(sys.argv) != 2:
    print "Format: python create_adjacency.py [schema name]"
    sys.exit()

if os.environ.get("GTFS_PASSWORD") is None:
    print "No system variables (GTFS_PASSWORD) set for password"
    sys.exit()

PASSWORD = os.environ.get("GTFS_PASSWORD")
schema_name = sys.argv[1]

conn = psycopg2.connect(database=DB_NAME, user=USER_NAME, password=PASSWORD, host=HOST, port=PORT)
cur = conn.cursor()

get_stop_times_query = "SELECT trip_id, stop_id, stop_sequence FROM " + schema_name + ".stop_times ORDER BY trip_id, stop_sequence"
cur.execute(get_stop_times_query)
data = cur.fetchall()
# pprint.pprint(data[:10])
# {
#     "lower_stop_id_higher_stopid": [route ids]
# }
adjacency = {}

for i in range(len(data)-1):
    stop1 = data[i]
    stop2 = data[i+1]
    if stop1[0] != stop2[0]:
        continue
    key = stop1[1] + "_" + stop2[1] if int(stop1[1]) < int(stop2[1]) else stop2[1] + "_" + stop1[1]
    if key not in adjacency:
        adjacency[key] = []
    if stop1[0] not in adjacency[key]:
        adjacency[key].append(stop1[0])

cur.execute("CREATE TABLE " + schema_name + ".stop_adjacency (stop_id_1 varchar(255), stop_id_2 varchar(255), trip_ids varchar(255)[]);")

# cur.execute("INSERT INTO " + schema_name + ".stop_adjacency (stop_id_1, stop_id_2, route_ids) VALUES (%s, %s, '{\"ASDF\"}');", ("asdf", "asdf"))
insert_values = ",".join(cur.mogrify("(%s, %s, %s)", (k.split("_")[0], k.split("_")[1], "{" + ", ".join(adjacency[k]) + "}")) for k in adjacency.keys())
# print insert_values
cur.execute("INSERT INTO " + schema_name + ".stop_adjacency (stop_id_1, stop_id_2, trip_ids) VALUES " + insert_values)
conn.commit()
cur.close()
conn.close()
