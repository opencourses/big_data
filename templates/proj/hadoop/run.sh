#!/bin/bash
set -e 

# This script moves the files in the filesystem and then run hadoop
# Remove folders of the previous run
hdfs dfs -rm -r /ex_name_data
hdfs dfs -rm -r /ex_name_out

# Put input data collection into hdfs
hdfs dfs -put /ex_name_data


# Run application
hadoop jar target/ex_name-1.0.0.jar it.polito.bigdata.hadoop.DriverImpl 1 ex_name_data/document.txt  ex_name_out




