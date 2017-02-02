#!/bin/bash
set -e 

base_dir=$(cd `dirname "$0"` && pwd -P)

# This script moves the files in the filesystem and then run hadoop
# Remove folders of the previous run
hdfs dfs -ls /exercise41_data > /dev/null && hdfs dfs -rm -r /exercise41_data
hdfs dfs -ls /exercise41_out1 > /dev/null && hdfs dfs -rm -r /exercise41_out1
hdfs dfs -ls /exercise41_out2 > /dev/null && hdfs dfs -rm -r /exercise41_out2

# Put input data collection into hdfs
hdfs dfs -copyFromLocal "$base_dir/exercise41_data" /exercise41_data


# Run application
hadoop jar "$base_dir/target/exercise41-1.0.0.jar" com.alangiu.bigdata.hadoop.DriverImpl \
                                                1 /exercise41_data  /exercise41_out1 /exercise41_out2
