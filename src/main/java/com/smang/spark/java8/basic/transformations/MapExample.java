package com.smang.spark.java8.basic.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * THe following class demonstrates a Map transformation
 *
 * In a map transformation, there is one to one mapping between elements of the source RDD
 * and the elements of the Target RDD. A function is executed on each element of the source RDD
 * and a Target RDD is created
 */
public class MapExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Map Example");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> myList = Arrays.asList(1,2,3,4,5);

        JavaRDD<Integer> intRDD = sc.parallelize(myList);
//multiply each element of the input RDD by 2
        JavaRDD<Integer> outRDD = intRDD.map(x -> x*2);

        outRDD.foreach(n ->System.out.println(n));
    }

}
