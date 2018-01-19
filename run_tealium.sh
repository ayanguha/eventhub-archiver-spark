rm nohup.out

#nohup spark-submit --packages com.databricks:spark-avro_2.11:3.2.0 tealium_parser.py  wasb://event-hub-archive@poystdragdlbdp02.blob.core.windows.net/svocdevbdpeventhub/svocdevbdp_eventhub_dc_05/2/2017/08/03/10/*/*  source_image2.tealium_data &


#nohup spark-submit --packages com.databricks:spark-avro_2.11:3.2.0 tealium_parser.py  "2017-08-23 02:00:00"  &
nohup spark-submit --conf spark.jars.packages=com.databricks:spark-avro_2.11:3.2.0 tealium_parser.py  "2017-08-23 03:00:00"  &


