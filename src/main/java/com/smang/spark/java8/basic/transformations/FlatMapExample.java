package com.smang.spark.java8.basic.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class demonstrates the FlatMap transformation
 *
 * => In a FlatTransformation, each element of the source RDD is mapped to one or more elements in the target RDD
 * => The number of elements in the target RDD will be equal to or greater than the number elements in the source RDD
 * => Usages: to Tokenize
 */
public class FlatMapExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("FlatMap Example");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> myList = Arrays.asList("Hello Java", "Hello Scala", "Hello Python");

        JavaRDD<String> srcRDD = sc.parallelize(myList);
//Split each element by the delimiter " "
        JavaRDD<String> tgtRDD = srcRDD.flatMap(x ->Arrays.asList(x.split(" ")).iterator());

        tgtRDD.foreach(x -> System.out.println(x));

    }
}
