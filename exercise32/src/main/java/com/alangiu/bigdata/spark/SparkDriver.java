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

		SparkConf conf = new SparkConf().setAppName("exercise32");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> readRDD = sc.textFile(inputPath);

		JavaPairRDD<String, Double> pairRDD = readRDD.mapToPair(new PairFunction<String, String, Double>() {

			@Override
			public Tuple2<String, Double> call(String arg0) throws Exception {
				String[] s = arg0.split(",");
				return new Tuple2<String, Double>(s[0], Double.parseDouble(s[2]));
			}
			
		});

		JavaPairRDD<String, Double> reducedRDD = pairRDD.reduceByKey(new Function2<Double, Double, Double>() {

			@Override
			public Double call(Double v1, Double v2) throws Exception {
				return v1 >= v2 ? v1 : v2;
			}
			
		});
		
		reducedRDD.saveAsTextFile(outputPath);
		
		sc.close();
	}
}
