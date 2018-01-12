package com.smang.spark.java8.pairrdd.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Demonstration od reduceByKey
 *
 * => An aggregation transformation for pair RDDs where all the values of the pairs with the same key are given to an associative function
 * calculating the result (eg, sum)
 *
 * => Reduce by key transformation first aggregates the values with the same key at the partion level before shuffling.
 * Hence it is more performant than Group BY Transformation
 */
public class ReduceByKeyExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ReduceByKeyExample").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> input = Arrays.asList(1,2,3,4,5,6);

        JavaRDD<Integer> srcRDD = sc.parallelize(input);
        JavaPairRDD<String, Integer> myPairRDD = srcRDD.mapToPair(x ->  (x%2==0)? new Tuple2<String,Integer>("Even",x):new Tuple2<String,Integer>("Odd",x));
        //myPairRDD.foreach(x -> System.out.println(x));
        JavaPairRDD<String, Integer> outputRDD = myPairRDD.reduceByKey((v1,v2) -> v1+v2 );
        outputRDD.foreach(x -> System.out.println(x));
    }

}
