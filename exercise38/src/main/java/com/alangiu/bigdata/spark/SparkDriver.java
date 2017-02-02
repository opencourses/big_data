package com.alangiu.bigdata.spark;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;

public class SparkDriver {

	private static final Integer CRITICAL_THRESHOLD = 3;
	
	public static void main(String[] args) {

		String logsPath = args[0];
		String neighborsPath = args[1];
		String outputPath = args[2];

		String outputPath1 = outputPath+"/output1";
		String outputPath2 = outputPath+"/output2";
		String outputPath3 = outputPath+"/output3";
		
		SparkConf conf = new SparkConf().setAppName("exercise38");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> logsRDD = sc.textFile(logsPath).cache();
		JavaRDD<String> neighborsRDD = sc.textFile(neighborsPath);

		/***** Phase one *********/
		JavaPairRDD<String, Count> logsPairRDD = logsRDD.mapToPair(new PairFunction<String, String, Count>() {
			@Override
			public Tuple2<String, Count> call(String arg0) throws Exception {
				String[] s = arg0.split(",");
				int critical = Integer.parseInt(s[5]) <= CRITICAL_THRESHOLD? 1 : 0;
				return new Tuple2<String, Count>(s[0], new Count(1, critical));
			}
		});
		
		logsPairRDD.reduceByKey(new Function2<Count, Count, Count>() {
			// calculate the number of total and critical elements for every station
			@Override
			public Count call(Count arg0, Count arg1) throws Exception {
				Count c = new Count(0, 0);
				c.total += arg0.total + arg1.total;
				c.critical += arg0.critical + arg1.critical;
				return c;
			}
		}).mapToPair(new PairFunction<Tuple2<String, Count>, String, Double>() {
			// Now calculate the percentage of critical situation over the total
			@Override
			public Tuple2<String, Double> call(Tuple2<String, Count> a) throws Exception {
				Double perc = new Double(a._2.critical)/ new Double(a._2.total);
				return new Tuple2<String, Double>(a._1, perc);
			}
			
		}).filter(new Function<Tuple2<String, Double>, Boolean>() {
			// filter just the percentage > 0.8
			@Override
			public Boolean call(Tuple2<String, Double> a) throws Exception {
				return a._2 > 0.8 ? true : false;
			}
		}).mapToPair(new PairFunction<Tuple2<String, Double>, Double, String>() {
			// invert the pair to sort them easily
			@Override
			public Tuple2<Double, String> call(Tuple2<String, Double> arg0) throws Exception {
				return new Tuple2<Double,String>(arg0._2, arg0._1);
			}
			
		}).sortByKey(false).saveAsTextFile(outputPath1);
		
		/********** Phase 2 *********/
		logsRDD.mapToPair(new PairFunction<String, String, Count>() {
			@Override
			public Tuple2<String, Count> call(String arg0) throws Exception {
				String[] str = arg0.split(",");
				Integer hour = Integer.parseInt(str[2]);
				Integer min_slot = 4*(hour/4);
				Integer max_slot = min_slot+3;
				String key = str[0]+"-["+min_slot+","+max_slot+"]";
				int critical = (Integer.parseInt(str[5]) <= CRITICAL_THRESHOLD ? 1 : 0);
				return new Tuple2<String, Count>(key, new Count(1, critical));
			}
		}).reduceByKey(new Function2<Count, Count, Count>() {
			@Override
			public Count call(Count arg0, Count arg1) throws Exception {
				Count c = new Count(0, 0);
				c.total += arg0.total + arg1.total;
				c.critical += arg0.critical + arg1.critical;
				return c;
			}
		}).mapToPair(new PairFunction<Tuple2<String, Count>, String, Double>() {
			// Now calculate the percentage of critical situation over the total
			@Override
			public Tuple2<String, Double> call(Tuple2<String, Count> a) throws Exception {
				Double perc = new Double(a._2.critical)/ new Double(a._2.total);
				return new Tuple2<String, Double>(a._1, perc);
			}
			
		}).filter(new Function<Tuple2<String, Double>, Boolean>() {
			// filter just the percentage > 0.8
			@Override
			public Boolean call(Tuple2<String, Double> a) throws Exception {
				return a._2 > 0.8 ? true : false;
			}
		}).mapToPair(new PairFunction<Tuple2<String, Double>, Double, String>() {
			// invert the pair to sort them easily
			@Override
			public Tuple2<Double, String> call(Tuple2<String, Double> arg0) throws Exception {
				return new Tuple2<Double,String>(arg0._2, arg0._1);
			}
			
		}).sortByKey(false).saveAsTextFile(outputPath2);
		
		/**************** Phase 3 ************/
		// filter all the values with full slots
		JavaRDD<String> filteredRDD = logsRDD.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String arg0) throws Exception {
				String[] a = arg0.split(",");
				return Integer.parseInt(a[5]) == 0? true: false;
			}
		});
		// make a pairRDD with key timestamp and value the string
		JavaPairRDD<String, String> timestampLogRDD = 
				filteredRDD.mapToPair(new PairFunction<String, String, String>() {
					@Override
					public Tuple2<String, String> call(String arg0) throws Exception {
						String[] str = arg0.split(",");
						String timestamp = str[1]+str[2]+str[3];
						return new Tuple2<String, String>(timestamp, arg0);
					}
		});
		// group by key
		JavaPairRDD<String, Iterable<String>> timestampLogGroupedRDD = 
				timestampLogRDD.groupByKey();
		// Create a neighbor HashMap
		final Map<String, List<String>> map = neighborsRDD.mapToPair(
				new PairFunction<String, String, List<String>>() {
			@Override
			public Tuple2<String, List<String>> call(String arg0) throws Exception {
				String[] s = arg0.split(",");
				// s[0] is the key
				String[] val = s[1].split("\\s+");
				List<String> valList = new ArrayList<>(Arrays.asList(val));
				return new Tuple2<String, List<String>>(s[0], valList);
			}
		}).collectAsMap();
		// recurr on the iterable and for every element verify if the neighbor are in the list,
		System.out.println("provaaa"+map.size());
		JavaRDD<String> finalRDD = timestampLogGroupedRDD.flatMap(
				new FlatMapFunction<Tuple2<String, Iterable<String>>,String>() {

			@Override
			public Iterable<String> call(Tuple2<String, Iterable<String>> arg0) throws Exception {
				List<String> a = new LinkedList<String>();
				a.add("prova");
				return a;
			}
			
		});
		finalRDD.saveAsTextFile(outputPath3);
		
//		JavaRDD<String> finalRDD = timestampLogGroupedRDD.flatMap(
//				new FlatMapFunction<Tuple2<String, Iterable<String>>, String>() {
//			@Override
//			public Iterable<String> call(Tuple2<String, Iterable<String>> arg0) throws Exception {
////				List<String> result = new ArrayList<>();
////				HashSet<String> set = new HashSet<>();
////				for (String s: arg0._2) {
////					set.add(s);
////				}
////				
////				for (String s: set) {
////					String[] vet = s.split(",");
////					if (!map.containsKey(vet[0])) {
////						continue;
////					}
//////					if (set.containsAll(map.get(vet[0]))) {
//////						result.add(s);
//////					}
////				}
////				if (result.isEmpty()) {
////					return null;
////				}
////				result.add("");
//				return null;
//			}
//		});
		// if yes add export a new string with the correct value
//		finalRDD.saveAsTextFile(outputPath3);
//		Long number = finalRDD.count();
//		System.out.println("The number of lines is "+number);
		
		sc.close();
	}
}
