package com.smang.spark.java8.basic.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class demonstrates the MapToFilter transformation with the help of an example
 *
 * => Similar to Map transformation, the function is applied to to each element of the source RDD, but produces
 * a pair RDD
 * => number of elements remains the same
 * => This transformation is specific to JavaRDDs. For other RDDs, the map transformation does both Map and MapToPair transformations
 * */
public class MapToPairExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("MapToPairExample").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> input = Arrays.asList(1,2,3,4,5,6);

        JavaRDD<Integer> srcRDD = sc.parallelize(input);
        JavaPairRDD<Integer, String> tgtRDD = srcRDD.mapToPair(x ->  (x%2==0)? new Tuple2<Integer,String>(x,"Even"):new Tuple2<Integer,String>(x,"Odd"));
        tgtRDD.foreach(x -> System.out.println(x));
    }
}
