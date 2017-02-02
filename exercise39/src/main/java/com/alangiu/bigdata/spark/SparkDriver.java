package com.alangiu.bigdata.spark;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String movie_list = args[0];
		String user_pref = args[1];
		String movie_data = args[2];
		String outputPath = args[3];
		final Double threshold = Double.parseDouble(args[4]);

		String outputPath1 = outputPath + "/output1";
		String outputPath2 = outputPath + "/output2";

		SparkConf conf = new SparkConf().setAppName("exercise39");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> movieListRDD = sc.textFile(movie_list);
		JavaRDD<String> userPrefRDD = sc.textFile(user_pref);
		JavaRDD<String> movieDataRDD = sc.textFile(movie_data);

		JavaPairRDD<String, String> movieUserRDD = movieListRDD.mapToPair(new PairFunction<String, String, String>() {
			// This is an RDD of <movieId, UserId>
			@Override
			public Tuple2<String, String> call(String arg0) throws Exception {
				String[] str = arg0.split(",");
				return new Tuple2<String, String>(str[1], str[0]);
			}
		});

		JavaPairRDD<String, String> movieGenreRDD = movieDataRDD.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String arg0) throws Exception {
				String[] str = arg0.split(",");
				return new Tuple2<String, String>(str[0], str[2]);
			}
		});

		JavaPairRDD<String, Tuple2<String, String>> tempRDD = movieUserRDD.join(movieGenreRDD);
		// temp RDD contains as key the movie Id and as value the user ID and
		// the movie-genre
		// I need to join again to have an RDD with key user-id and value
		// movie-genre
		JavaPairRDD<String, String> userGenreRDD = tempRDD
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, String>>, String, String>() {

					@Override
					public Tuple2<String, String> call(Tuple2<String, Tuple2<String, String>> arg0) throws Exception {
						return new Tuple2<String, String>(arg0._2._1, arg0._2._2);
					}
				});

		JavaPairRDD<String, String> userPrefPairRDD = userPrefRDD.mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String arg0) throws Exception {
				String[] str = arg0.split(",");
				return new Tuple2<String, String>(str[0], str[1]);
			}
		});

		JavaPairRDD<String, Tuple2<String, String>> result = userGenreRDD.join(userPrefPairRDD);
		JavaPairRDD<String, Count> flattenedRDD = result.combineByKey(new Function<Tuple2<String, String>, Count>() {
			@Override
			public Count call(Tuple2<String, String> arg0) throws Exception {
				int critical = (arg0._1.equals(arg0._2)) ? 0 : 1;
				return new Count(1, critical);
			}
		}, new Function2<Count, Tuple2<String, String>, Count>() {
			@Override
			public Count call(Count arg0, Tuple2<String, String> arg1) throws Exception {
				int critical = (arg1._1.equals(arg1._2)) ? 0 : 1;
				arg0.total += 1;
				arg0.critical += critical;
				return arg0;
			}
		}, new Function2<Count, Count, Count>() {
			@Override
			public Count call(Count arg0, Count arg1) throws Exception {
				Count c = new Count(0, 0);
				c.total += arg0.total + arg1.total;
				c.critical += arg0.critical + arg1.critical;
				return c;
			}
		});

		JavaPairRDD<String, Double> percentageRDD = flattenedRDD.mapValues(new Function<Count, Double>() {
			@Override
			public Double call(Count c) throws Exception {
				Double percentage = new Double(c.critical) / new Double(c.total);
				return percentage;
			}
		}).filter(new Function<Tuple2<String, Double>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, Double> arg0) throws Exception {
				return arg0._2 >= threshold ? true : false;
			}
		});
		percentageRDD.saveAsTextFile(outputPath1);

		/******* Phase 2 ******/
		// Select for each user that has a misleading profile
		// I need two pairRDD to be cogrouped togeather
		// one is JavaPairRDD<UserID, MovieGenre> generated merging the watched
		// movies and the movie data
		// I already generated it and is "userGenreRDD"
		// I also need a JavaPairRDD<UserId, MovieGenre> generated from the user
		// preferences and
		// I already generated it and is "userPrefPairRDD"

		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> cogroupRDD = userGenreRDD
				.cogroup(userPrefPairRDD);
		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> validUserRDD = cogroupRDD
				.subtractByKey(percentageRDD);
		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> misleadingUser = cogroupRDD
				.subtractByKey(validUserRDD);
		// In misleading User I have the user that are misleading, but now I
		// have to flattend the map
		JavaPairRDD<String, String> finalResult2 = misleadingUser.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>, String, String>() {
					
					@Override
					public Iterable<Tuple2<String, String>> call(
							Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> arg0) throws Exception {
						List<Tuple2<String, String>> result = new ArrayList<>();
						Iterable<String> watched = arg0._2._1;
						Iterable<String> liked = arg0._2._2;
						HashSet<String> likedSet = new HashSet<>();
						HashMap<String, Integer> watchedMap = new HashMap<>();
						for (String l : liked) {
							likedSet.add(l);
						}
						for (String w: watched) {
							if (likedSet.contains(w)) {
								// the user likes this category so it's not important
								continue;
							}
							if (!watchedMap.containsKey(w)) {
								watchedMap.put(w, 1);
							} else {
								int value = watchedMap.get(w);
								watchedMap.put(w, value+1);
							}
						}
						for (Entry<String,Integer> e: watchedMap.entrySet()) {
							if (e.getValue() >= 5) {
								result.add(new Tuple2<String, String>(arg0._1, e.getKey()));
							}
						}
						return result;
					}
				});
		
		finalResult2.saveAsTextFile(outputPath2);
		
		sc.close();
	}
}
