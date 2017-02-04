#!/bin/bash
set -e 

base_dir=$(cd `dirname "$0"` && pwd -P)

# This script moves the files in the filesystem and then run hadoop
# Remove folders of the previous run
hdfs dfs -ls /exercise48_data > /dev/null && hdfs dfs -rm -r /exercise48_data
hdfs dfs -ls /exercise48_out1 > /dev/null && hdfs dfs -rm -r /exercise48_out1
hdfs dfs -ls /exercise48_out2 > /dev/null && hdfs dfs -rm -r /exercise48_out2

# Put input data collection into hdfs
hdfs dfs -copyFromLocal "$base_dir/exercise48_data" /exercise48_data


# Run application
hadoop jar "$base_dir/target/exercise48-1.0.0.jar" com.alangiu.bigdata.hadoop.DriverImpl \
                                                1 /exercise48_data  /exercise48_out1 /exercise48_out2
