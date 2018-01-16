package com.smang.spark.java8.basic.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Demonstrates Collect action.
 * Collect retrieves all the elements of the RDD from all its partions and bring it to driver program.
 *
 *Collect should be called oly on small RDDs as the bringing in huge volume of data to the driver program crashes the driver
 */
public class CollectExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("CollectExample").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> inputRDD = sc.parallelize(Arrays.asList(1,2,3,4));

        inputRDD.collect().forEach(x -> System.out.println(x));

    }
}
