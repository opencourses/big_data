#!/bin/bash
set -e 

base_dir=$(cd `dirname "$0"` && pwd -P)

# This script moves the files in the filesystem and then run hadoop
# Remove folders of the previous run
hdfs dfs -ls /exercise11_data > /dev/null && hdfs dfs -rm -r /exercise11_data
hdfs dfs -ls /exercise11_out > /dev/null && hdfs dfs -rm -r /exercise11_out

# Put input data collection into hdfs
hdfs dfs -copyFromLocal "$base_dir/exercise11_data" /exercise11_data


# Run application
hadoop jar "$base_dir/target/exercise11-1.0.0.jar" com.alangiu.bigdata.hadoop.DriverImpl \
                                                1 /exercise11_data  /exercise11_out
