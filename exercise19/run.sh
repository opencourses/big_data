#!/bin/bash
set -e 

base_dir=$(cd `dirname "$0"` && pwd -P)

# This script moves the files in the filesystem and then run hadoop
# Remove folders of the previous run
hdfs dfs -ls /exercise19_data > /dev/null && hdfs dfs -rm -r /exercise19_data
hdfs dfs -ls /exercise19_out > /dev/null && hdfs dfs -rm -r /exercise19_out

# Put input data collection into hdfs
hdfs dfs -copyFromLocal "$base_dir/exercise19_data" /exercise19_data


# Run application
hadoop jar "$base_dir/target/exercise19-1.0.0.jar" com.alangiu.bigdata.hadoop.DriverImpl \
                                                1 /exercise19_data/input_file /exercise19_data/stopwords  /exercise19_out
