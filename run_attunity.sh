rm nohup.out
sliceDt,depth,storageAccountName,container,eventHubNameSpace,eventHubEntity
nohup spark-submit --packages com.databricks:spark-avro_2.11:3.2.0 --executor-memory 2g --num-executors 5 attunity_parser.py "2017-10-16 00:00:00"  "hour" "poystdragdlbdp02"  "event-hub-archive" "svocdevbdpeventhub02" "attunity"  &


