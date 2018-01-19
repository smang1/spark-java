package com.smang.spark.java8.storage.fileFormats;

import com.smang.spark.java8.storage.fileFormats.pojos.Contact;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;

/**
 * Reads and Writes specially formatted data from text file
 *  Normal -> using MAp
 *  Optimized -> using MapPartition , it returns an iterator of elements of the RDD for each partition
 *
 *  The number of output files = the number of partitions used;
 *  Use repartition(n) method to change the number of output files => EXPENSIVE as it shuffles the data
 *  Use Coalesce instead for better performance => Optimal as no shuffling is done
 */
public class ReadWriteSpeciallyFormattedText {
    final static Logger logger = Logger.getLogger(ReadWriteSpeciallyFormattedText.class);

    public static void main(String[] args) {
        String inputFile = "data/in/contacts.csv";
        String outputPathNormal = "data/out/ReadWriteSpeciallyFormattedText";
        String outputPathOptimized = "data/out/ReadWriteSpeciallyFormattedTextOptimized";


        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("ReadWriteSpeciallyFormattedText").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Read the data from a single file
        JavaRDD<String> input = sc.textFile(inputFile);
        logger.info(String.format("Number of lines in the file <user1>: %s", input.count()));

        //Splitting each line using custom delimiter and parsing using a POJO class
        JavaRDD<Contact> contacts = input.map(line -> {
            String[] parts = line.split("~");
            return new Contact(parts[0], Integer.parseInt(parts[1]), parts[2]);
        });


        //writing the data to a textfile (writes the object if toString() method is not overridden in the POJO class )
        contacts.saveAsTextFile(outputPathNormal);

        //Optimized way of parsing data by using Map Partion
        JavaRDD<Contact> contactsUsingMP = input.mapPartitions(line ->{
            ArrayList<Contact> contactsMP = new ArrayList<>();
            while(line.hasNext()){
                String[] partsMP = line.next().split("~");
                contactsMP.add( new Contact(partsMP[0], Integer.parseInt(partsMP[1]), partsMP[2]));
            }
            return contactsMP.iterator();
        });

        //writing the data to a textfile (writes the object if toString() method is not overridden in the POJO class )
        contactsUsingMP.saveAsTextFile(outputPathOptimized);




    }
}
