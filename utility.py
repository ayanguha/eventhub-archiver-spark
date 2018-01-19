from pyspark import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import sys,datetime,ConfigParser


'''
Objective:
1. Recursively go through Event Hub Capture path and create an Avro Dataframe using com.databricks:spark-avro_2.11:3.2.0 package
2. Unpack EH Capture Avro output as per following : https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-capture-overview
3. Extract Body (Actual EH msg) and cast it to String

'''

def slice2partition(sliceDT):
    return sliceDT.replace("-","").replace(" ","").replace(":","")

def createSparkSession(appname="Bupa pyspark App"):
    spark = SparkSession \
           .builder \
           .appName(appname) \
           .enableHiveSupport() \
           .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")    
           #.config("spark.jars.packages","com.databricks:spark-avro_2.11:3.2.0") \
    spark.conf.set("spark.sql.shuffle.partitions",50)    
    spark._jsc.hadoopConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive","true")
    spark._jsc.hadoopConfiguration().set("hive.exec.dynamic.partition","true")
    spark._jsc.hadoopConfiguration().set("hive.exec.dynamic.partition.mode","nonrestrict")
    return spark

def extractDT(sliceDT):
    # Expected Format: YYYY-MM-DD HH:MI:SS
    d = datetime.datetime.strptime(sliceDT,"%Y-%m-%d %H:%M:%S")
    return (d.strftime("%Y"),d.strftime("%m"),d.strftime("%d"),d.strftime("%H"))

def getTargetDateTimeDir(sliceDT):
    d = datetime.datetime.strptime(sliceDT,"%Y-%m-%d %H:%M:%S")
    return d.strftime("%Y/%m/%d/%H")


def getFileLocation(sliceDt,depth,storageAccountName,container,eventHubNameSpace,eventHubEntity):
    base = "wasb://<container>@<storageAccountName>.blob.core.windows.net/<eventHubNameSpace>/<eventHubEntity>"
    yr,mth,dy,hr = extractDT(sliceDt)
    if depth == "hour":
        file_location = base + "/*/<year>/<month>/<day>/<hour>/*/*" 
    elif depth == "day":
        file_location = base + "/*/<year>/<month>/<day>/*/*/*"
    elif depth == "month":
        file_location = base + "/*/<year>/<month>/*/*/*/*"
    elif depth == "year":
        file_location = base + "/*/<year>/*/*/*/*/*"
    
    final_file_loc = file_location \
                    .replace("<year>",yr)  \
                    .replace("<month>",mth)  \
                    .replace("<day>",dy) \
                    .replace("<hour>",hr) \
                    .replace("<storageAccountName>",storageAccountName) \
                    .replace("<container>",container) \
                    .replace("<eventHubNameSpace>",eventHubNameSpace) \
                    .replace("<eventHubEntity>",eventHubEntity)

    return final_file_loc  

def getBase(sparkSess,hdfs_path):
    '''
    Unpack the avro format. Output will have EH Capture audit information AND a column called "Body" which holds actual msg data in binary
    '''
    df = sparkSess.read.format("com.databricks.spark.avro").load(hdfs_path)
    return df    

def getunpackBody(sparkSess,baseDF):
    '''
    Cast Binary "Body" field to a string
    '''
    bodyDF = baseDF.select(baseDF["body"].cast("string").alias("body"),baseDF["SequenceNumber"],baseDF["Offset"],baseDF["EnqueuedTimeUtc"] )
    #bodyDF.show(truncate=False)
    return bodyDF

if __name__ == "__main__":
    print getFileLocation("attunity-l2", "2017-10-05 00:00:00")
