package com.smang.spark.java8.basic.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * This class demonstrates the Filter transformation
 * <p>
 * => In a filter transformation , a funtion is executed on each element of the input RDD and
 * only those elements for which the function returns ture are selected to for the target RDD
 * => The number of elements in the target RDD will be less than or equal to the number of elements
 * of the source RDD
 */
public class FilterExample {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Filter Example");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> myList = Arrays.asList(1, 2, 3, 4, 5);

        JavaRDD<Integer> intRDD = sc.parallelize(myList);
//Filter out even numbers
        JavaRDD<Integer> outRDD = intRDD.filter(x -> x % 2 == 0);

        outRDD.foreach(n -> System.out.println(n));
    }
}
