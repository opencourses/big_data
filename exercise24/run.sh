#!/bin/bash
set -e 

base_dir=$(cd `dirname "$0"` && pwd -P)

# This script moves the files in the filesystem and then run hadoop
# Remove folders of the previous run
hdfs dfs -ls /exercise24_data > /dev/null && hdfs dfs -rm -r /exercise24_data
hdfs dfs -ls /exercise24_out > /dev/null && hdfs dfs -rm -r /exercise24_out

# Put input data collection into hdfs
hdfs dfs -copyFromLocal "$base_dir/exercise24_data" /exercise24_data


# Run application
hadoop jar "$base_dir/target/exercise24-1.0.0.jar" com.alangiu.bigdata.hadoop.DriverImpl \
                /exercise24_data/document.txt  /exercise24_data/dictionary.txt /exercise24_out
