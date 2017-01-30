#!/bin/bash
set -e 

base_dir=$(cd `dirname "$0"` && pwd -P)

# This script moves the files in the filesystem and then run hadoop
# Remove folders of the previous run
hdfs dfs -ls /exercise8_data > /dev/null && hdfs dfs -rm -r /exercise8_data
hdfs dfs -ls /exercise8_out > /dev/null && hdfs dfs -rm -r /exercise8_out
hdfs dfs -ls /exercise8_out2 > /dev/null && hdfs dfs -rm -r /exercise8_out2

# Put input data collection into hdfs
hdfs dfs -copyFromLocal "$base_dir/exercise8_data" /exercise8_data


# Run application
hadoop jar "$base_dir/target/exercise8-1.0.0.jar" com.alangiu.bigdata.hadoop.DriverImpl \
                                                1 /exercise8_data  /exercise8_out /exercise8_out2
