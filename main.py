import json
import pymysql

#read config file
config_file = open("app_config.json", "r", encoding='utf-8-sig', newline='\r\n')
configuration = json.loads(config_file.read())
config_file.close()

#get config data
dbcconfig = configuration["dbcconfig"]

#connect to db
try:
    dbc = pymysql.connect(**dbcconfig)
except mysql.connector.Error as err:
    print(err)

#query
cursor = dbc.cursor()
query = "SELECT zugnummerfull, evanr FROM zuege LIMIT 10"

cursor.execute(query)
#output
for (zugnummerfull, evanr) in cursor:
    print("{},{}".format(zugnummerfull, evanr))

#clean up
dbc.close()