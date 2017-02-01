package com.alangiu.bigdata.spark;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		String outputPath = args[1];

		SparkConf conf = new SparkConf().setAppName("exercise30");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> readRDD = sc.textFile(inputPath);

		JavaDoubleRDD pollutionRDD = readRDD.mapToDouble(new DoubleFunction<String>() {

			@Override
			public double call(String arg0) throws Exception {
				String[] s = arg0.split(",");
				return Double.parseDouble(s[2]);
			}
		});
		
		final Double max = pollutionRDD.max();
		
		JavaRDD<String> finalRDD = readRDD.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String arg0) throws Exception {
				String[] a = arg0.split(",");
				if (max.equals(Double.parseDouble(a[2]))){
					return true;
				}
				return false;
			}
			
		}).map(new Function<String, String>() {

			@Override
			public String call(String arg0) throws Exception {
				String[] a = arg0.split(",");
				return a[1];
			}
		}).distinct();
	
		finalRDD.saveAsTextFile(outputPath);

		sc.close();
	}
}
