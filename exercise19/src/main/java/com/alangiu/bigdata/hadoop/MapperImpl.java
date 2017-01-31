package com.alangiu.bigdata.hadoop;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperImpl extends Mapper<
                    LongWritable, Text,
                    LongWritable, Text> {
    
	private HashSet<String> set;
	
	@SuppressWarnings("deprecation")
	@Override
	protected void setup(Context context) throws IOException {
		Path[] caches = context.getLocalCacheFiles();
		BufferedReader br = new BufferedReader(new FileReader(caches[0].toString()));
		
		set = new HashSet<>();
		
		String line = "";
		while ((line = br.readLine()) != null) {
			set.add(line.toLowerCase());
		}
		br.close();
	}
	
	@Override
    protected void map(LongWritable key, Text value,
            Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split("\\s+");
		String out = "";
		for (String v: values) {
			if (!set.contains(v.replaceAll(",|\\.|;|:|\\s", "").toLowerCase())) {
				out+=v+" ";
			}
		}
		context.write(key, new Text(out));
    }
}
