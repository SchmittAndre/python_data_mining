import json
import pymysql

import query_suite


#read config file
CONFIG_FILE_NAME = "app_config.json"

config_file = open(CONFIG_FILE_NAME, "r", encoding='utf-8-sig', newline='\r\n')
configuration = json.loads(config_file.read())
config_file.close()


#get config data
dbcconfig = configuration["dbcconfig"]


#connect to db
try:
    dbc = pymysql.connect(**dbcconfig)
except pymysql.connector.Error as err:
    print(err)

    
#setup query suite
qs = query_suite.QuerySuite();
qs.use_dbc(dbc)
qs.set_limit(5000)


#r = qs.get_zugid_like(dailytripid="-100020256270627274", yymmddhhmm="", stopindex="")
#r = qs.get_zuege_by_zugid("8898709046814622615-1711301719-2")
#r = qs.get_stationname_by_evanr("8000107")
r = qs.get_ttsid_on_trip(
    dailytripid="-5016615278318514860",
    yymmddhhmm="1712011704")
print(r)
for x in r:
    #print(x[0])
    zug = qs.get_zug_by_zugid(x[0])
    zug = qs.select(zug, columns=[1,9])
    evanr = qs.get_stationname_by_evanr(zug[0][1])
    print(zug, evanr)


#clean up
dbc.close()

