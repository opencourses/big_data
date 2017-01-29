#!/bin/bash
set -e 

base_dir=$(cd `dirname "$0"` && pwd -P)

# This script moves the files in the filesystem and then run hadoop
# Remove folders of the previous run
hdfs dfs -ls /exercise4_data > /dev/null && hdfs dfs -rm -r /exercise4_data
hdfs dfs -ls /exercise4_out > /dev/null && hdfs dfs -rm -r /exercise4_out

# Put input data collection into hdfs
hdfs dfs -copyFromLocal "$base_dir/exercise4_data" /exercise4_data


# Run application
hadoop jar "$base_dir/target/exercise4-1.0.0.jar" com.alangiu.bigdata.hadoop.DriverImpl \
                                                1 /exercise4_data  /exercise4_out
