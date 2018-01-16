package com.smang.spark.java8.basic.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Demonstrates Top action
 * Fetches TOP n elements from an RDD, after natural ordering
 * or custom ordering based on the comparator hive as a parameter
 * Natural ordering, sorts the elements in the ascending order and fetches the n largest elements
 *
 *
 */
public class TopExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Top Example").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> inputRDD = sc.parallelize(Arrays.asList(15000, 99999, 8765, 1, 2, 3, 4, 5, 2, 6, 5, 11, 28, 19, 71, 90, 100));
        List<Integer> topValuesWithoutExplicitOrdering=inputRDD.top(5);
        List<Integer> topValuesNaturalOrdering=inputRDD.top(5, Comparator.naturalOrder());
        List<Integer> topValuesReverseOrdering=inputRDD.top(5, Comparator.reverseOrder());
        System.out.println(String.format("topValuesWithoutExplicitOrdering: %s,\n topValuesNaturalOrdering: %s,\n, topValuesReverseOrdering: %s\n",topValuesWithoutExplicitOrdering, topValuesNaturalOrdering, topValuesReverseOrdering));
    }
}
