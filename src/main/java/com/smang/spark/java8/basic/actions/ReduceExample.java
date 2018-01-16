package com.smang.spark.java8.basic.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Demonstrates Reduce with an example
 * Passes the elements of a RDD to a function, and returns the computed result
 * For Reduce action, the type of the elements of the RDD and the type of the computed type should be the same
 * eg: Sum, multiplication, count etc
 *
 */
public class ReduceExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Reduce Example").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> inputRDD = sc.parallelize(Arrays.asList( 1, 2, 3, 4, 5, 2, 6, 5, 6, 7,8, 9, 10));
        Integer sum = inputRDD.reduce((v1, v2) ->v1+v2);
        System.out.println(String.format("sum: %s,\n",sum));
    }
}
