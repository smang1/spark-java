package com.smang.spark.java8.exceptionHandling;

import com.smang.spark.java8.accumulators.StringAccumulator;
import com.smang.spark.java8.storage.fileFormats.pojos.PersonalDetails;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class RejectionExampleWithFilterAndAccumulator {

    public static void main(String[] args) {
        String inputFile = "data/in/PersonalDetailsERR.json";
        String outputPath = "data/out/PersonalDetailsWithoutERR.json";
        String errorOnly = "data/out/PersonalDetailsWithoutERR.json";

        SparkConf conf = new SparkConf().setAppName("ReadWriteSpeciallyFormattedText").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        ArrayList<String> accumulatorInitial = new ArrayList<String>();
        Accumulator<List<String>> rejections = sc.accumulator(accumulatorInitial,"rej", new StringAccumulator() );

        JavaRDD<String> input = sc.textFile(inputFile);

        //Get the correct dat to an RDD
        JavaRDD<PersonalDetails> correctData = input.filter(line -> {
            try {
                new ObjectMapper().readValue(line, PersonalDetails.class);
                return true;
            } catch (Exception e) {
                // As the accumulator isused within a transformation, This accumulator coupld be updated more than once
                // and can have undesirable effects
                rejections.add(Arrays.asList(String.format("Rejected the row %s due to the error '%s'", line, e.getMessage())));
                return false;
            }
        }).map(line -> new ObjectMapper().readValue(line, PersonalDetails.class));
        correctData.foreach(line -> System.out.println(line.getFirstName()));

        log.info("Rejected lines: " + rejections.value());


    }
}
