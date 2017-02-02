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

		SparkConf conf = new SparkConf().setAppName("exercise33");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> readRDD = sc.textFile(inputPath);

		JavaRDD<String> filteredRDD = readRDD.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String arg0) throws Exception {
				String[] s = arg0.split(",");
				if (Double.parseDouble(s[2]) > 50) {
					return true;
				}
				return false;
			}
		});
		
		JavaPairRDD<String, Double> pairRDD = filteredRDD.mapToPair(new PairFunction<String, String, Double>() {

			@Override
			public Tuple2<String, Double> call(String arg0) throws Exception {
				String[] s = arg0.split(",");
				return new Tuple2<String, Double>(s[0], Double.parseDouble(s[2]));
			}
		});
		
		JavaPairRDD<String, Integer> reducedRDD = 
				pairRDD.combineByKey(new Function<Double, Integer>() {

					@Override
					public Integer call(Double arg0) throws Exception {
						return 1;
					}
					
				}, new Function2<Integer, Double, Integer>() {

					@Override
					public Integer call(Integer arg0, Double arg1) throws Exception {
						return arg0+1;
					}

					
				}, new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer arg0, Integer arg1) throws Exception {
						return arg0+arg1;
					}
					
				});

		JavaPairRDD<String, Integer> finalRDD = reducedRDD.filter(new Function<Tuple2<String, Integer>, Boolean>() {

			@Override
			public Boolean call(Tuple2<String, Integer> arg) throws Exception {	
				return arg._2 > 2 ? true : false;
			}
		});
		
		finalRDD.saveAsTextFile(outputPath);
		
		sc.close();
	}
}
