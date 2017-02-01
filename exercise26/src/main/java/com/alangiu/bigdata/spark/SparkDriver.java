package com.alangiu.bigdata.spark;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		String outputPath = args[1];

		SparkConf conf = new SparkConf().setAppName("exercise26");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> readRDD = sc.textFile(inputPath);

		JavaRDD<String> finalRDD = readRDD.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				if (s.contains("www.google.com")) {
					return true;
				}
				return false;
			}
		}).map(new Function<String, String>() {
			@Override
			public String call(String s) throws Exception {
				String[] strings = s.split("--");
				return strings[0];
			}			
		});

		finalRDD.saveAsTextFile(outputPath);

		sc.close();
	}
}
