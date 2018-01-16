package com.smang.spark.java8.basic.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Like reduce action, fold also passes the elements of an RDD to an aggregate function and
 * returns the computed result of the same type as that of the input elements
 * However fold requires a "Zero element/Identity element" that is utilized while initializing the calls to the partitions of an RDD
 * For Addition, this element value can be 0, for multiplication this value could be 1 etc*/
public class FoldExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Fold Example").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> inputRDD = sc.parallelize(Arrays.asList( 1, 2, 3, 4, 5, 6, 7,8, 9, 10));
        Integer sum = inputRDD.fold(0,(v1, v2) ->v1+v2);
        System.out.println(String.format("sum: %s\n",sum));
    }
}
