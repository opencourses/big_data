#!/bin/bash
set -e 

base_dir=$(cd `dirname "$0"` && pwd -P)

# This script moves the files in the filesystem and then run hadoop
# Remove folders of the previous run
hdfs dfs -ls /exercise43_data > /dev/null && hdfs dfs -rm -r /exercise43_data
hdfs dfs -ls /exercise43_out1 > /dev/null && hdfs dfs -rm -r /exercise43_out1
hdfs dfs -ls /exercise43_out2 > /dev/null && hdfs dfs -rm -r /exercise43_out2

# Put input data collection into hdfs
hdfs dfs -copyFromLocal "$base_dir/exercise43_data" /exercise43_data


# Run application
hadoop jar "$base_dir/target/exercise43-1.0.0.jar" com.alangiu.bigdata.hadoop.DriverImpl \
                                                1 /exercise43_data  /exercise43_out1 /exercise43_out2
