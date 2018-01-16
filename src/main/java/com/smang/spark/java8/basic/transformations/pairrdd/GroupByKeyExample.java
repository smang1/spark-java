package com.smang.spark.java8.basic.transformations.pairrdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * This class demonstrates groupByKey transformation with an example
 *  => groups all the values with the same key
 *  => It transforms pair RDD with elements (k,v) to (k, Iterable(v))
 *
 * => very costly operation as it includes a lot of shuffling of data
 *
 */
public class GroupByKeyExample {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("GroupByKeyExample").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> input = Arrays.asList(1,2,3,4,5,6);

        JavaRDD<Integer> srcRDD = sc.parallelize(input);
        JavaPairRDD<String, Integer> myPairRDD = srcRDD.mapToPair(x ->  (x%2==0)? new Tuple2<String,Integer>("Even",x):new Tuple2<String,Integer>("Odd",x));
        //myPairRDD.foreach(x -> System.out.println(x));
        JavaPairRDD<String, Iterable<Integer>> outputRDD = myPairRDD.groupByKey();
        outputRDD.foreach(x -> System.out.println(x));
    }
}
