package com.alangiu.bigdata.spark;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		String outputPath = args[1];

		SparkConf conf = new SparkConf().setAppName("exercise25");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> readRDD = sc.textFile(inputPath);

		JavaRDD<String> finalRDD = readRDD.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String s) throws Exception {
				if (s.contains("google")) {
					return true;
				}
				return false;
			}

		});

		finalRDD.saveAsTextFile(outputPath);

		sc.close();
	}
}
