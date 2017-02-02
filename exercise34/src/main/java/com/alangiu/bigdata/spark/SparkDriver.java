package com.alangiu.bigdata.spark;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		String outputPath = args[1];

		SparkConf conf = new SparkConf().setAppName("exercise34");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> readRDD = sc.textFile(inputPath);

		
		JavaRDD<String> filteredRDD = readRDD.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String arg0) throws Exception {
				String[] s = arg0.split(",");
				return Double.parseDouble(s[2]) > 50? true: false;
			}
		});
		
		JavaPairRDD<String, String> pairRDD = filteredRDD.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String arg0) throws Exception {
				String[] s = arg0.split(",");
				return new Tuple2<String, String>(s[0], s[1]);
			}
		});
		
		JavaPairRDD<String, Iterable<String>> finalRDD = pairRDD.groupByKey();
				
		finalRDD.saveAsTextFile(outputPath);

		sc.close();
	}
}
