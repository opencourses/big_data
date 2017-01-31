#!/bin/bash
set -e 

base_dir=$(cd `dirname "$0"` && pwd -P)

# This script moves the files in the filesystem and then run hadoop
# Remove folders of the previous run
hdfs dfs -ls /exercise23_data > /dev/null && hdfs dfs -rm -r /exercise23_data
hdfs dfs -ls /exercise23_out1 > /dev/null && hdfs dfs -rm -r /exercise23_out1
hdfs dfs -ls /exercise23_out2 > /dev/null && hdfs dfs -rm -r /exercise23_out2

# Put input data collection into hdfs
hdfs dfs -copyFromLocal "$base_dir/exercise23_data" /exercise23_data


# Run application
hadoop jar "$base_dir/target/exercise23-1.0.0.jar" com.alangiu.bigdata.hadoop.DriverImpl \
                                                1 /exercise23_data  /exercise23_out1 /exercise23_out2
