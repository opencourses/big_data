package com.alangiu.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperImpl extends Mapper<
                    Text, Text,
                    Text, Text> {
    
    protected void map(Text key, Text value,
            Context context) throws IOException, InterruptedException {

    }
}
