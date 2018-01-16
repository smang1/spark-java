package com.smang.spark.java8.basic.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Demonstrates aggregate action
 * Does aggregation similar to reduce and fold, however for an aggregate function, the type of the elements of the RDD on which
 * the computation is applied and the type of the result returned need not be the same. plus, it takes 3 parameters
 *  zero value: An identity value is used while initializing calls to to each partition and also once while initialising the combiner function
 *              The type of the identity element should be the same as that of the final result
 *  Accumulator function: function applied on each element of a partition
 *  Combiner function: Function used to combine the  results from the accumulators
 */
public class AggregateExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Aggregate Example").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> inputRDD = sc.parallelize(Arrays.asList( 1, 2, 3, 4, 5, 6, 7,8, 9, 10),3); //3 partitions
        System.out.println("Number of partitions for the input RDD: " + inputRDD.getNumPartitions());
        String concatValue = inputRDD.aggregate("X", (x,y) -> x+y, (x,z) -> x+z);
        System.out.println(String.format("concatValue: %s\n",concatValue));
    }
}
