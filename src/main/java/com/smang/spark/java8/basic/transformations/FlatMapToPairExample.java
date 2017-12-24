package com.smang.spark.java8.basic.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


import java.util.Arrays;
import java.util.List;

/**
 * This class demonstrates the use of FlatMapToPair transformation with an example
 *
 * => Similar to mapToPair transformation, but the target RDD for this transformation is a Pair RDD
 */
public class FlatMapToPairExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("FlatMapToPairExample");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> myList = Arrays.asList("Hello Java", "Hello Scala", "I like to program in java");
        JavaRDD<String> input = sc.parallelize(myList);
        JavaPairRDD<String,Integer> output = input.flatMapToPair(x -> Arrays.asList(new Tuple2<String,Integer>(x,x.split(" ").length)).iterator());
        output.foreach(x-> System.out.println(x));
    }

}
