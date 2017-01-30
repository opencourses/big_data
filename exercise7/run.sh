#!/bin/bash
set -e 

base_dir=$(cd `dirname "$0"` && pwd -P)

# This script moves the files in the filesystem and then run hadoop
# Remove folders of the previous run
hdfs dfs -ls /exercise7_data > /dev/null && hdfs dfs -rm -r /exercise7_data
hdfs dfs -ls /exercise7_out > /dev/null && hdfs dfs -rm -r /exercise7_out

# Put input data collection into hdfs
hdfs dfs -copyFromLocal "$base_dir/exercise7_data" /exercise7_data


# Run application
hadoop jar "$base_dir/target/exercise7-1.0.0.jar" com.alangiu.bigdata.hadoop.DriverImpl \
                                                1 /exercise7_data  /exercise7_out
