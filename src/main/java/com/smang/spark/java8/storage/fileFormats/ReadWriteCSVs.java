package com.smang.spark.java8.storage.fileFormats;

import com.smang.spark.java8.storage.fileFormats.pojos.Movie;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

public class ReadWriteCSVs {

    private JavaRDD<Movie> movies;

    public static void main(String[] args) {
        String inputFile1 = "data/in/movies1.csv"; // contains"," within data
        String inputFile2 = "data/in/movies2.csv"; // Does not contain  "," within data
        String outputPath = "data/out/ReadWriteCSVs";
        SparkSession ss = SparkSession.builder()
                .master("local").appName("ReadWriteCSVs")
                .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse").getOrCreate();

        JavaRDD<Movie> movies = ss.read().textFile(inputFile1).javaRDD()
                .filter(line -> !line.equals("id,name,genre")).filter(line -> !line.isEmpty()).filter(line -> line != null) //filtering the header and empty lines
                .map(line -> Movie.parseMovies(line));
        System.out.println("Number of lines in the file: "+ movies.count());
        movies.foreach(m -> System.out.println(String.format("id: %s, title: %s, genre: %s",m.getId(), m.getTitle(),m.getGenre()))); //Will fail as there are commas within the data

        Dataset<Row> csvData = ss.read().format("com.databricks.spark.csv").option("header", true).option("inferSchema", true) // reading the file using CSV library (ignores , withing quotes) + inferring schema from header
                .load(inputFile2);
        System.out.println("Number of line in the dataset csv Data:"+ csvData.count());
        csvData.printSchema();
        csvData.show();

        StructType customSchema = new StructType(new StructField[]{
           new StructField("movieId", DataTypes.LongType, true, Metadata.empty()),
                new StructField("movieTitle", DataTypes.StringType, true, Metadata.empty()),
                new StructField("movieGenre", DataTypes.StringType, true, Metadata.empty()),

        });
        Dataset<Row> csvDataCustomSchema = ss.read().format("com.databricks.spark.csv").option("header", true) // reading the file using CSV library (ignores , withing quotes)
                .schema(customSchema)
                .load(inputFile2);
        System.out.println("Number of line in the dataset csv Data:"+ csvDataCustomSchema.count());
        csvDataCustomSchema.printSchema();
        csvDataCustomSchema.show();

        csvDataCustomSchema.write().format("com.databricks.spark.csv").option("header", true)
                .option("codec","org.apache.hadoop.io.compress.GzipCodec"). save(outputPath);








    }
}
