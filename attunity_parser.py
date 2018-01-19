from pyspark import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from multiprocessing.dummy import Pool as ThreadPool
import json
from collections import OrderedDict,namedtuple
from pyspark.sql import Row
import sys,os,datetime


'''
Objective:
1. Recursively go through Event Hub Capture path and create an Avro Dataframe using com.databricks:spark-avro_2.11:3.2.0 package
2. Unpack EH Capture Avro output as per following : https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-capture-overview
3. Extract Body (Actual EH msg) and cast it to String (Only MsgType = DT (Attunity Specific))
4. Extract distinct table names from all the messages
5. Read Configured table & column metadata from schema.json file
5. Go through the list of tables found in data (ie in Step (4)) and filter out un-configured tables. This is to safeguard any failure in case any new table is introduced at source
6. For each table which is found in data and is configured in schema.json, table extraction & processing is done in parallel.
   Parallel processing is handled by Multiprocessing Threadpools.
   Number of threads in a pool: 1-10.
   Number of Pool: numberOfTables/10 + 1

7. table extraction & processing function:

   a) Extract Attunity Specific fields from Msg: data, beforedata, operation, changeSequence, timestamp, streamPosition, transactionId
   b) From Data, JSON Decoder is used to convert json string to python Ordered dictionary.
   c) Add Event Hub related audit fields to the dictionary.
   d) Add Attunity audit fields to the dictionary.
   e) Add derived field as per business logic to the dictionary. Please see respective function for more details.
   f) Convert the dict to a namedtuple and then to a Row object (in order to keep column order)
   g) Finally create a dataframe (using toDF)
   h) Insert data frame to Hive (Append)
'''

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
    bodyDF = baseDF.select(baseDF["body"].cast("string").alias("body"),baseDF["SequenceNumber"],baseDF["Offset"],baseDF["EnqueuedTimeUtc"] ).cache()
    return bodyDF





def main():
    spark = createSparkSession("Attunity EH Archiver to Data Lake")
    sliceDt = sys.argv[1]
    depth = sys.argv[2]
    storageAccountName = sys.argv[3].lower()
    container =  sys.argv[4].lower()
    eventHubNameSpace = sys.argv[5].lower()
    eventHubEntity = sys.argv[6].lower()
    schemaFile = sys.argv[7]

    file_location = getFileLocation(sliceDt,depth,storageAccountName,container,eventHubNameSpace,eventHubEntity)
    print file_location

    base = getBase(spark,file_location)
    ub = getunpackBody(spark,base)
    successfull = []
    failed = []
    if ub.count() > 0:
        # Attunity specific stuff
        tableList = getTableList(spark,ub)
        
        print "Current Table List: %s" %(tableList)

        sch = getConfiguredTableList(spark,schemaFile)


        for tnameRow in tableList:
            t = tnameRow['tname'].split('~')[0]
            op = tnameRow['tname'].split('~')[1]
            tsch = getConfiguredTableSchemaDefinition(sch,t)
            if tsch:
                try:
                    getunpackBodyData(spark,ub,t,tsch,op, sliceDt)
                    print "Successful: %s" %(t)
                    successfull.append(t)
                except Exception as inst:
                    print "Failed: %s" %(t)
                    failed.append(inst)
                    raise

        if len(failed) > 0:
            for f in failed:
                print f.message
            raise Exception("Failed: Attunity EH Archiver to Data Lake")


    else:
        print "Source file %s and all subdirectories are Empty" %(file_location)
    spark.stop()

def getPool(numberOfTables):
    poolFactor = 10
    return ThreadPool((numberOfTables/poolFactor)+1)

def getConfiguredTableList(spark,schemaFile):
    schemaString=spark.sparkContext.wholeTextFiles(schemaFile).map(lambda t: t[1]).collect()[0]
    return json.loads(schemaString)["feedList"]


def getConfiguredTableSchemaDefinition(schemaStructList,tableName):

    for k in schemaStructList:
       if k["feed"]["sourceTableName"].lower() == tableName.lower():
           return k["feed"]



def getunpackBodyDataWrapper(tup):
    sparkSession = tup[0]
    bodyDF = tup[1]
    tablename = tup[2]
    feedSchemaDefinition = tup[3]
    return getunpackBodyData(sparkSession,bodyDF,tablename, feedSchemaDefinition)



def getTableList(sparkSess,unpackBody):
    '''
    Identify tables which are present in data.
    @Attunity-Specific
    '''
    unpackBody.registerTempTable("unpackBodyTemp")
    tableList = sparkSess.sql("select distinct concat(get_json_object(body,'$.message.data.schema_table'),'~',get_json_object(body,'$.message.headers.operation'))  tname from unpackBodyTemp").collect()
    print tableList
    return tableList


def prepFinalPayload(payload,row,isDeleted=False):
    payload.append(isDeleted)
    '''
    # Add All Audit columns
    @EH-Specific
    '''

    
    payload.append(row["SequenceNumber"])
    payload.append(row["Offset"])
    payload.append(row["EnqueuedTimeUtc"])
    '''
    # Add All Audit columns
    @Attunity-Specific
    '''
    payload.append(row["operation"])
    payload.append(row["changeSequence"])
    payload.append(row["timestamp"])
    payload.append(row["streamPosition"])
    payload.append(row["transactionId"])
    #r = namedtuple('tableschema', payload.keys())(**payload)
    return payload


def extractDataJson(data,columnList):
    out = []
    NOT_FOUND_VALUE = 'COL NOT IN SOURCE'
    d = json.loads(data)
    for k in columnList:
        try:
            out.append(d[k])
        except:
            print 'Non Terminating Error: %s not in source' %(k)
            out.append(NOT_FOUND_VALUE) 
            
    return out

def applyBusinessLogic(businessDateField,columnList):
    '''
    A trick for closure to work. Required to pass additional arguements into a function which is also used in map API
    @Attunity-Specific
    '''
    def _createFinalpayload(row):
        data = row["data"]
        beforeData = row["beforeData"]

        '''
        Decode Json but keep the column order. Beforedata may or may not be available, so try-catch it

        '''

        d = extractDataJson(data,columnList)

        try:
            bd = extractDataJson(beforeData,columnList)
        
        except:
            bd = {businessDateField : None}

        res = []

        '''
        1. IF Operation = "INSERT" THEN Load data with isDelete=False
        2. IF Operation = "UPDATE" AND BeforeData.START_DATE <> data.START_DATE THEN Load
               a. 1 record with before data with isDelete=True so that it can cancel the one which got updated.
               b. 1 record with data with isDelete=False
        3. IF Operation = "UPDATE" AND BeforeData.START_DATE = data.START_DATE THEN Load data with isDelete=False
        4. IF Operation = "DELETE" THEN Load data with isDelete=True
        5. IF Operation = "REFRESH" THEN Delete entire table and load data with isDeleted=False.

        (1), (3) and (5) are hanlded by default else clause below.

        '''
        if row["operation"] == "DELETE":
            pld = prepFinalPayload(d,row,isDeleted=True)
            res.append(pld)
        elif businessDateField and row["operation"] == "UPDATE" and ( json.loads(data)[businessDateField] <>  json.loads(beforeData)[businessDateField]):
            pld = prepFinalPayload(d,row,isDeleted=False)
            bpld = prepFinalPayload(bd,row,isDeleted=True)
            res.append(pld)
            res.append(bpld)
        else:  ### row["operation"] == "REFRESH" or row["operation"] == "INSERT":
            pld = prepFinalPayload(d,row,isDeleted=False)
            res.append(pld)


        return res

    return _createFinalpayload

def tableSchemaType(feedSchemaDefinition):
    audit_fields = ["isDeleted","audit_eventhub_SequenceNumber","audit_eventhub_Offset","audit_eventhub_EnqueuedTimeUtc","audit_msg_operation","audit_msg_changeSequence","audit_msg_timestamp","audit_msg_streamPosition","audit_msg_transactionId"]
    s = StructType()
    for x in feedSchemaDefinition["schema"]:
        s.add(x["columnName"],StringType(),True)

    for x in audit_fields:
        s.add(x,StringType(),True)
    columnList = [x["columnName"] for x in feedSchemaDefinition["schema"]]
    if feedSchemaDefinition["isBusinessDateUpdatePossible"]:
        bussDate = [x["columnName"] for x in feedSchemaDefinition["schema"] if x["isBusinessDate"]][0]
    else:
        bussDate = None


    return (s,columnList,bussDate)


def getunpackBodyData(sparkSess,unpackBody,tablename, feedSchemaDefinition,op, sliceDt):

    '''
    Extract attunity specific values
    @Attunity-Specific
    '''

    schema, columnList, bussDate = tableSchemaType(feedSchemaDefinition)
    targetSourceImageTable = feedSchemaDefinition["targetSourceImageTable"]
    unpackBody.registerTempTable("unpackBodyTemp")
    runSql = '''select get_json_object(body,'$.message.data') data,
                       get_json_object(body,'$.message.beforeData') beforeData,
                       get_json_object(body,'$.message.headers.operation') operation ,
                       get_json_object(body,'$.message.headers.changeSequence') changeSequence ,
                       get_json_object(body,'$.message.headers.timestamp') timestamp ,
                       get_json_object(body,'$.message.headers.streamPosition') streamPosition ,
                       get_json_object(body,'$.message.headers.transactionId') transactionId,
                       SequenceNumber, Offset, EnqueuedTimeUtc
              from unpackBodyTemp
             where lower(get_json_object(body,'$.message.data.schema_table')) = lower('<table_name>')
             '''.replace('<table_name>',tablename)

    unpackBodyFullMsg = sparkSess.sql(runSql)
    

    unpackBodyData = unpackBodyFullMsg.rdd.flatMap(applyBusinessLogic(bussDate,columnList)).toDF(schema).cache()

    '''
    IF Operation = "REFRESH" THEN Delete entire table and load data again

    '''

    print "Writing to: %s, Number of Records to write: %s" %(targetSourceImageTable,unpackBodyData.count())

    if op == "REFRESH":
        unpackBodyData.write.insertInto(targetSourceImageTable,overwrite=True)
    else:
        unpackBodyData.write.insertInto(targetSourceImageTable,overwrite=False)

    print "Unpersisting..."
    unpackBodyData.unpersist()


if __name__ == "__main__":
    main()
