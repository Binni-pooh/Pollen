# Pollen 
## Run instructions
1. Download this 2 jars into /opt/spark-4.2.0-bin-hadoop3/jars
>wget https://repo1.maven.org/maven2/io/delta/delta-spark_4.2_2.13/4.4.1/delta-spark_4.2_2.13-4.4.1.jar
>wget https://repo1.maven.org/maven2/io/delta/delta-storage/4.4.1/delta-storage-4.4.1.jar
2. Run this command to start Spark
>/opt/spark-4.2.0-bin-hadoop3/bin/spark-submit     --class org.apache.spark.sql.connect.service.SparkConnectServer     --master "local[*]"     --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension     --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
3. Run main.py
```bash
python main.py
```