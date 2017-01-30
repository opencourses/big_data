package com.alangiu.bigdata.hadoop;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapperImpl extends Mapper<
                    LongWritable, Text,
                    LongWritable, DoubleWritable> {
	
	final SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd");
	
	@Override
    protected void map(LongWritable key, Text value,
            Context context) throws IOException, InterruptedException {
    	String[] values = value.toString().split("\\s+");
    	try {
    		Date date = parser.parse(values[0]);
    		Calendar c = new GregorianCalendar();
        	c.setTime(date);
        	c.set(Calendar.DAY_OF_MONTH, 0);
        	Double d = Double.parseDouble(values[1]);
        	context.write(new LongWritable(c.getTimeInMillis()), new DoubleWritable(d));
    	} catch (ParseException e){
    		throw new InterruptedException();
    	}
    }
}
