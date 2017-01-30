#!/bin/bash
set -e 

base_dir=$(cd `dirname "$0"` && pwd -P)

# This script moves the files in the filesystem and then run hadoop
# Remove folders of the previous run
hdfs dfs -ls /exercise12_data > /dev/null && hdfs dfs -rm -r /exercise12_data
hdfs dfs -ls /exercise12_out > /dev/null && hdfs dfs -rm -r /exercise12_out

# Put input data collection into hdfs
hdfs dfs -copyFromLocal "$base_dir/exercise12_data" /exercise12_data


# Run application
hadoop jar "$base_dir/target/exercise12-1.0.0.jar" com.alangiu.bigdata.hadoop.DriverImpl \
                                                /exercise12_data  /exercise12_out 30.0
