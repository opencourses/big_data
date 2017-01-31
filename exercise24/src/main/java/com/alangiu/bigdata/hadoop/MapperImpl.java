package com.alangiu.bigdata.hadoop;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperImpl extends Mapper<
                    LongWritable, Text,
                    LongWritable, Text> {
	
	private HashMap<String, Integer> map;
	
	@SuppressWarnings("deprecation")
	@Override
	protected void setup(Context context) throws IOException {
		map = new HashMap<>();
		Path[] paths = context.getLocalCacheFiles();
		BufferedReader br = new BufferedReader(new FileReader(paths[0].toString()));
		String line = null;
		while ((line = br.readLine()) != null) {
			String[] v = line.split("\\s+");
			map.put(v[1], Integer.parseInt(v[0]));
		}
		br.close();
	}
    
	@Override
    protected void map(LongWritable key, Text value,
            Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split("\\s+");
		String out = "";
		for (String v: values) {
			Integer i = map.get(v);
			out +=i+" ";
		}
		context.write(key, new Text(out));
    }
}
