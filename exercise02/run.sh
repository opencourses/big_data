#!/bin/bash
set -e 

base_dir=$(cd `dirname "$0"` && pwd -P)

# This script moves the files in the filesystem and then run hadoop
# Remove folders of the previous run
hdfs dfs -ls /exercise2_data > /dev/null && hdfs dfs -rm -r /exercise2_data
hdfs dfs -ls /exercise2_out > /dev/null && hdfs dfs -rm -r /exercise2_out

# Put input data collection into hdfs
hdfs dfs -copyFromLocal "$base_dir/exercise2_data" /exercise2_data


# Run application
hadoop jar "$base_dir/target/exercise2-1.0.0.jar" com.alangiu.bigdata.hadoop.DriverImpl \
                                        1 /exercise2_data  /exercise2_out




