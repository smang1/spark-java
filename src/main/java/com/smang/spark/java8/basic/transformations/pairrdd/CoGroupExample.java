package com.smang.spark.java8.basic.transformations.pairrdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * This class demonstrates the coGroup transformation
 * if two pair RDDs of type <X,Y> and <X,Z> are transformed using coGroup transformation,
 * we get an RDD of type <X, Tuple2<Iterable<Y>, Iterable<Z>>>
 */
public class CoGroupExample {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("CoGroup Example").setMaster("local");
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

        JavaPairRDD <String, Tuple2<Iterable<String>, Iterable<Integer>>> outputRDD = inputPairRDD1.cogroup(inputPairRDD2);

        outputRDD.foreach(x -> System.out.println(x));

    }
}
