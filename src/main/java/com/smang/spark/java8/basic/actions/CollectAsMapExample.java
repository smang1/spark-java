package com.smang.spark.java8.basic.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

/**
 * demonstrates collect as map action
 *
 * Retreieves all elements an RDD from all its partitions and brings it to the driver program.
 *  ! Call only on small RDDs
 *
 *  When the result is collected as map, if the same key is present twice, one gets overwritten by the other
 */
public class CollectAsMapExample {

    public static void main(String[] args) {
            SparkConf conf = new SparkConf().setAppName("CollectAsMapExample").setMaster("local");
            JavaSparkContext sc = new JavaSparkContext(conf);

            JavaPairRDD<String, Integer> inputPairRDD = sc.parallelizePairs(Arrays.asList(
                    new Tuple2<String, Integer>("A", 1),
                    new Tuple2<String, Integer>("C", 3),
                    new Tuple2<String, Integer>("X", 24),
                    new Tuple2<String, Integer>("E", 5),
                    new Tuple2<String, Integer>("A", 11),
                    new Tuple2<String, Integer>("J", 10)
                    )
            );

          Map<String,Integer> myMap= inputPairRDD.collectAsMap();

          myMap.forEach((x,y) -> System.out.println(String.format("(%s,%s)",x,y)));

    }
}
