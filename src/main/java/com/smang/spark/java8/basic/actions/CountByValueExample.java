package com.smang.spark.java8.basic.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * Demonstrates countByValue action
 * counts the occurence of each value/element in an RDD
 * Applicable for both simple RDDs and Pair RDDs
 * For a simple RDD, it counts the occurrence of elements with specific value and returns a map with RDD elements as keys
 * and their respective number of occurrences as values
 *
 * In the case of a pair RDD, it counts the number of occurrences of unique elements, say a pair, and returns a map with
 * the key the element of the source RDD (a tuple) and value a long value which is equal to the number of occurrences of the element
 */
public class CountByValueExample {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("CountByValue Example").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> inputRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 2, 6, 5, 1, 2, 1, 1, 90, 100));

        JavaPairRDD<String, Integer> inputPairRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("A", 1),
                new Tuple2<String, Integer>("A", 2),
                new Tuple2<String, Integer>("A", 1),
                new Tuple2<String, Integer>("B", 1),
                new Tuple2<String, Integer>("C", 2),
                new Tuple2<String, Integer>("D", 4),
                new Tuple2<String, Integer>("X", 3),
                new Tuple2<String, Integer>("M", 3),
                new Tuple2<String, Integer>("P", 10)
                )
        );
        Map<Integer, Long> outMap1 = inputRDD.countByValue();
        System.out.println(Collections.singletonList(outMap1));

        Map<Tuple2<String,Integer>, Long> outMap2=inputPairRDD.countByValue();
        System.out.println(Collections.singletonList(outMap2));

    }

}

