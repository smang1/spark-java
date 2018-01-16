package com.smang.spark.java8.basic.transformations.pairrdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.Arrays;

/**
 * This example demonstrates the Join transformation (JOIN, Left outer Join and Right outer Join)
 * => Joins two Pair RDDs of type <X,Y> and <X,Z> resulting in a pair RDD <X, (Y,Z)>
 */
public class JoinExample {


    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JoinExample").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, String> inputPairRDD1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, String>("A", "a"),
                new Tuple2<String, String>("B", "b"),
                new Tuple2<String, String>("C", "C"),
                new Tuple2<String, String>("X", "x"),
                new Tuple2<String, String>("C", "c")
                )
        );

        JavaPairRDD<String, Integer> inputPairRDD2 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("A", 1),
                new Tuple2<String, Integer>("C", 3),
                new Tuple2<String, Integer>("X", 24),
                new Tuple2<String, Integer>("E", 5),
                new Tuple2<String, Integer>("A", 11),
                new Tuple2<String, Integer>("J", 10)
                )
        );

        JavaPairRDD<String, Tuple2<String, Integer>> joinRDD = inputPairRDD1.join(inputPairRDD2);
        joinRDD.foreach(x -> System.out.println(x));


        inputPairRDD1.rightOuterJoin(inputPairRDD2);
        JavaPairRDD<String, Tuple2<String, Optional<Integer>>> lJoinRDD = inputPairRDD1.leftOuterJoin(inputPairRDD2);
        lJoinRDD.foreach(x -> System.out.println(x));

        JavaPairRDD<String, Tuple2<Optional<String>, Integer>> rJoinRDD = inputPairRDD1.rightOuterJoin(inputPairRDD2);
        rJoinRDD.foreach(x -> System.out.println(x));


    }
}
