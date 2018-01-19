from pyspark import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from multiprocessing.dummy import Pool as ThreadPool
import json
from collections import OrderedDict,namedtuple
from pyspark.sql import Row
import sys
import  utility

''' 
Objective:
1. Recursively go through Event Hub Capture path and create an Avro Dataframe using com.databricks:spark-avro_2.11:3.2.0 package
2. Unpack EH Capture Avro output as per following : https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-capture-overview
3. As this application is specific to tealium, Data Source is hard coded
'''

TEALIUM_COLUMN_LIST = ["event_action","event_category","event_label","event_name","meta.viewport","meta.format-detection","meta.description","meta.application-name","meta.msapplication-TileColor","meta.msapplication-TileImage","dom.referrer","dom.title","dom.domain","dom.query_string","dom.hash","dom.url","dom.pathname","dom.viewport_height","dom.viewport_width","ut.domain","ut.version","ut.event","ut.session_id","ut.account","ut.profile","ut.env","ut.visitor_id","page_name_hier","page_name","page_type","user_agent","ancilliary_cover_level","ancilliary_product_description","ancilliary_product_id","hospital_cover_level","hospital_product_description","hospital_product_id","cover_code","cover_description","cover_type","member_id_hash","suffix_id","logged_in_state","event_name","channel","site_server","pre_start","arrears_flag"]


      
DATA_SOURCE = "tealium"
TARGET_TABLE_NAME= "source_image2.tealium_data"
def main():
    sliceDt = sys.argv[1]
    depth = sys.argv[2]
    storageAccountName = sys.argv[3]
    container =  sys.argv[4]
    eventHubNameSpace = sys.argv[5]
    eventHubEntity = sys.argv[6]

    file_location = utility.getFileLocation(sliceDt,depth,storageAccountName,container,eventHubNameSpace,eventHubEntity)

    print file_location

    spark = utility.createSparkSession("Tealium EH Archiver to Data Lake")
    base = utility.getBase(spark,file_location)
    ub = utility.getunpackBody(spark,base)
    if ub.count() > 0:    
        unpackBodyData = spark.read.json((ub.rdd.map(extractData)))
        unpackBodyData.write.saveAsTable(TARGET_TABLE_NAME,mode="append")
    else:
        print "Source file %s and all subdirectories are Empty" %(file_location)
    spark.stop()

def extractDT(sliceDT):
    # Expected Format: YYYY-MM-DD HH:MI:SS
    d = datetime.datetime.strptime(sliceDT,"%Y-%m-%d %H:%M:%S")    
    return (d.strftime("%Y"),d.strftime("%m"),d.strftime("%d"),d.strftime("%H"))



def extractData(row):
    dd = {}
    d = json.loads(row["body"],object_pairs_hook=OrderedDict)
    for k in TEALIUM_COLUMN_LIST:
        if not d.has_key(k) or d[k] == "" or not d[k]:
            v = "NV"
        else:
            v = d[k]
        dd[cleanupFieldNames(k)] = v

    dd["fullmsg"] = row["body"]
    r = json.dumps(dd)
    return r

def fillValue(v):
    if not v or v == "":
        v = "No Value"
    return v

def cleanupFieldNames(s):
    s = s.strip(" ")
    if s.startswith("_"):
        s = s[1:]
    alnumonly = [ch if ch.isalnum() else "_" for ch in s]

    return "".join(alnumonly)
    

if __name__ == "__main__":
    main()
