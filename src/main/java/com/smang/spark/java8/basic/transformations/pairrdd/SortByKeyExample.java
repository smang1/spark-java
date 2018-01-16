package com.smang.spark.java8.basic.transformations.pairrdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SortByKeyExample {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SortByKeyExample").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Integer> inputPairRDD = sc.parallelizePairs(Arrays.asList(new Tuple2<String, Integer>("X", 1),
                new Tuple2<String, Integer>("P", 10),
                new Tuple2<String, Integer>("A", 11),
                new Tuple2<String, Integer>("x", 1),
                new Tuple2<String, Integer>("n", 2)
                )
        );
        JavaPairRDD<String, Integer> outputPairRDD = inputPairRDD.sortByKey();
        outputPairRDD.foreach(x -> System.out.println(x));
        JavaPairRDD<String, Integer> outputPairRDDDescending = inputPairRDD.sortByKey(false);
        outputPairRDDDescending.foreach(x -> System.out.println(x));

    }
}
