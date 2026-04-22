#!/bin/bash
echo "Waiting for NameNode to exit safe mode..."
until docker exec namenode hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; do
  sleep 3
done
echo "Creating HDFS directories..."
docker exec namenode hdfs dfs -mkdir -p /user/hive/warehouse
docker exec namenode hdfs dfs -mkdir -p /user/spark
docker exec namenode hdfs dfs -chmod -R 777 /user/hive/warehouse
docker exec namenode hdfs dfs -chmod -R 777 /user/spark
echo "HDFS directories ready."
