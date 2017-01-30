#!/bin/bash
set -e 

base_dir=$(cd `dirname "$0"` && pwd -P)

# This script moves the files in the filesystem and then run hadoop
# Remove folders of the previous run
hdfs dfs -ls /exercise9_data > /dev/null && hdfs dfs -rm -r /exercise9_data
hdfs dfs -ls /exercise9_out > /dev/null && hdfs dfs -rm -r /exercise9_out

# Put input data collection into hdfs
hdfs dfs -copyFromLocal "$base_dir/exercise9_data" /exercise9_data


# Run application
hadoop jar "$base_dir/target/exercise9-1.0.0.jar" com.alangiu.bigdata.hadoop.DriverImpl \
                                                1 /exercise9_data  /exercise9_out
