package com.alangiu.bigdata.spark;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		String outputPath = args[1];

		SparkConf conf = new SparkConf().setAppName("exercise42");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> readRDD = sc.textFile(inputPath);

		JavaRDD<String> reviewsRDD = readRDD.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String arg0) throws Exception {
				String[] s = arg0.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				if (s.length != 6) {
					return false;
				}
				return true;
			}
			
		}).map(new Function<String, String>() {

			@Override
			public String call(String arg0) throws Exception {
				String[] s = arg0.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				return s[4];
			}
			
		});
				
		JavaPairRDD<String, Integer> counterPairRDD = reviewsRDD.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {

			@Override
			public Iterator<Tuple2<String, Integer>> call(String arg0) throws Exception {
				List<Tuple2<String, Integer>> list = new ArrayList<>();
				String[]words = arg0.replaceAll(",|\\.|;|:|\\?|!|-|_", "").split("\\s+");
				for (int i = 0; i<words.length-1; i++) {
					list.add(new Tuple2<String, Integer>(words[i].toLowerCase()+" "+words[i+1].toLowerCase(), 1));
				}
 				return list.iterator();
			}
		});
		
		JavaPairRDD<String, Integer> finalRDD = counterPairRDD.combineByKey(new Function<Integer, Integer>(){
			@Override
			public Integer call(Integer arg0) throws Exception {
				return arg0;
			}
		}, new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0+arg1;
			}
		}, new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0+arg1;
			}
		});
		
		finalRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> arg0) throws Exception {
				return new Tuple2<Integer,String>(arg0._2, arg0._1);
			}
			
		}).sortByKey(false).saveAsTextFile(outputPath);
		
		sc.close();
	}
}
