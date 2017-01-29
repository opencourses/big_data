#!/bin/bash
set -e
ex_name=$1
ex_dir=$2
sed -i "" -e "s/ex_name/$ex_name/g" "$ex_dir/pom.xml"
sed -i "" -e "s/ex_proj/$ex_name/g" "$ex_dir/pom.xml"
mv "$ex_dir/ex_data" $ex_dir/$ex_name"_data"
sed -i "" "s/ex_name/$ex_name/g" "$ex_dir/run.sh"
sed -i "" "s/ex_name/$ex_name/g" "$ex_dir/src/main/java/com/alangiu/bigdata/hadoop/DriverImpl.java"
