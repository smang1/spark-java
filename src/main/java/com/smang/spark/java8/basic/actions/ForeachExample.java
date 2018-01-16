package com.smang.spark.java8.basic.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Demonstrates Foreach action with an example
 *
 * helps to perform a repetitive operation on each element of an RDD without bringing it to the Driver program
 */
public class ForeachExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("FoeEach Example").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> inputRDD = sc.parallelize(Arrays.asList( 1, 2, 3, 4, 5, 6, 7,8, 9, 10),3); //3 partitions
        System.out.println("Number of partitions for the input RDD: " + inputRDD.getNumPartitions());
        inputRDD.foreach(x -> System.out.println("The elements of the RDD are::"+ x));
    }
}
