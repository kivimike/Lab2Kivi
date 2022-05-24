from kafka import KafkaConsumer
from cassandra.io.libevreactor import LibevConnection
from cassandra.cluster import Cluster

cluster = Cluster()
cluster.connection_class = LibevConnection
session = cluster.connect('my_keyspace')
session.execute("CREATE TABLE IF NOT EXISTS my_keyspace.raw_logs (id int PRIMARY KEY, data text)")
session.execute("CREATE TABLE IF NOT EXISTS my_keyspace.temperature (key text PRIMARY KEY, value int)")
session.execute("CREATE TABLE IF NOT EXISTS my_keyspace.pressure (key text PRIMARY KEY, value int)")
session.execute("CREATE TABLE IF NOT EXISTS my_keyspace.humidity (key text PRIMARY KEY, value int)")
#session.execute("CREATE TABLE IF NOT EXISTS my_keyspace.hum (id int PRIMARY KEY, data text)")

consumer = KafkaConsumer('testtopic', bootstrap_servers=['localhost:9092'])
for msg in consumer:
     #print(msg.value.decode('utf-8'), msg.offset)
     session.execute("INSERT INTO my_keyspace.raw_logs (id, data) VALUES (%s, %s)", (msg.offset, msg.value.decode('utf-8')))
