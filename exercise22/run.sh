#!/bin/bash
set -e 

base_dir=$(cd `dirname "$0"` && pwd -P)

# This script moves the files in the filesystem and then run hadoop
# Remove folders of the previous run
hdfs dfs -ls /exercise22_data > /dev/null && hdfs dfs -rm -r /exercise22_data
hdfs dfs -ls /exercise22_out > /dev/null && hdfs dfs -rm -r /exercise22_out

# Put input data collection into hdfs
hdfs dfs -copyFromLocal "$base_dir/exercise22_data" /exercise22_data


# Run application
hadoop jar "$base_dir/target/exercise22-1.0.0.jar" com.alangiu.bigdata.hadoop.DriverImpl \
                                                1 /exercise22_data  /exercise22_out
