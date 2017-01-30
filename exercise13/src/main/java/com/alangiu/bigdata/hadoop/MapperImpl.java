package com.alangiu.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperImpl extends Mapper<
                    Text, Text,
                    NullWritable, Text> {
    
	private int top1 = 0;
	private Text top1_value = new Text();
	private int top2 = 0;
	private Text top2_value = new Text();
	
	@Override
    protected void map(Text key, Text value,
            Context context) throws IOException, InterruptedException {
		int v = Integer.parseInt(value.toString().trim());
		if (v >= top2) {
			if (v <= top1) {
				top2 = v;
				top2_value=key;
			} else {
				top1 = v;
				top1_value=key;
			}
		}
    }
	
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(NullWritable.get(), new Text(top1+"_"+top1_value+"_"+top2+"_"+top2_value));
	}
}
