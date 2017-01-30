#!/bin/bash
set -e 

base_dir=$(cd `dirname "$0"` && pwd -P)

# This script moves the files in the filesystem and then run hadoop
# Remove folders of the previous run
hdfs dfs -ls /exercise10_data > /dev/null && hdfs dfs -rm -r /exercise10_data
hdfs dfs -ls /exercise10_out > /dev/null && hdfs dfs -rm -r /exercise10_out

# Put input data collection into hdfs
hdfs dfs -copyFromLocal "$base_dir/exercise10_data" /exercise10_data


# Run application
hadoop jar "$base_dir/target/exercise10-1.0.0.jar" com.alangiu.bigdata.hadoop.DriverImpl \
                                                /exercise10_data  /exercise10_out
