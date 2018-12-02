package com.smang.spark.java8.jobs;

import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import fr.jetoile.hadoopunit.test.hdfs.HdfsUtils;
import fr.jetoile.hadoopunit.test.kafka.KafkaProducerUtils;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
//TODO Make the test work
public class StreamingDataTest {
    static private Logger LOGGER = LoggerFactory.getLogger(StreamingDataTest.class);
    static private Configuration configuration;

    @BeforeClass
    public static void setup() throws BootstrapException, NotFoundServiceException {
        try {
            System.out.println("Initializing configs");
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
            System.out.println("Initialized configs");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
      //  HadoopBootstrap.INSTANCE.startAll();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        HadoopBootstrap.INSTANCE.stopAll();

    }


    @Test
    @Ignore
    public void hdfsShouldStart() throws Exception {

        FileSystem hdfsFsHandle = HdfsUtils.INSTANCE.getFileSystem();


        FSDataOutputStream writer = hdfsFsHandle.create(new Path(configuration.getString(HadoopUnitConfig.HDFS_TEST_FILE_KEY)));
        writer.writeUTF(configuration.getString(HadoopUnitConfig.HDFS_TEST_STRING_KEY));
        writer.close();

        // Read the file and compare to test string
        FSDataInputStream reader = hdfsFsHandle.open(new Path(configuration.getString(HadoopUnitConfig.HDFS_TEST_FILE_KEY)));
        assertEquals(reader.readUTF(), configuration.getString(HadoopUnitConfig.HDFS_TEST_STRING_KEY));
        reader.close();
        hdfsFsHandle.close();

        URL url = new URL(
                String.format("http://localhost:%s/webhdfs/v1?op=GETHOMEDIRECTORY&user.name=guest",
                        configuration.getInt(HadoopUnitConfig.HDFS_NAMENODE_HTTP_PORT_KEY)));
        URLConnection connection = url.openConnection();
        connection.setRequestProperty("Accept-Charset", "UTF-8");
        BufferedReader response = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String line = response.readLine();
        response.close();
        //assertThat("{\"Path\":\"/user/guest\"}").isEqualTo(line);
    }

    @Test
    public void Test_createSparkStreamFromKafka() throws InterruptedException {
        String sparkApplicationName = "testing_streams";
        String sparkMasterUrl = "local[*]";
        long batchDuration = 1L;
        System.out.println("INSIDE THE TEST");

        SparkConf sparkConf = new SparkConf().setAppName(sparkApplicationName).setMaster(sparkMasterUrl).set("spark.driver.allowMultipleContexts", "true" );
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchDuration));
        String kafkaServer = configuration.getString(HadoopUnitConfig.ZOOKEEPER_HOST_KEY) + ":" + configuration.getInt(HadoopUnitConfig.ZOOKEEPER_PORT_KEY);
        System.out.println("kafkaServer: " + kafkaServer);

        for (int i = 0; i < 1; i++) {
            String payload = generateMessge(i);
            System.out.println("Sending message:" + i + " " + payload);
            KafkaProducerUtils.INSTANCE.produceMessages(configuration.getString(HadoopUnitConfig.KAFKA_TEST_TOPIC_KEY), String.valueOf(i), payload);
        }

/*
        KafkaConsumerUtils.INSTANCE.consumeMessagesWithNewApi(configuration.getString(HadoopUnitConfig.KAFKA_TEST_TOPIC_KEY), 10);
        System.out.println("KafkaConsumerUtils.INSTANCE.getNumRead():" + KafkaConsumerUtils.INSTANCE.getNumRead());
        // Assert num of messages produced = num of message consumed
        //Assert.assertEquals(configuration.getLong(HadoopUnitConfig.KAFKA_TEST_MESSAGE_COUNT_KEY), KafkaConsumerUtils.INSTANCE.getNumRead());
        StreamingData sd = new StreamingData();

        System.out.println("Connecting to kafkaServer: " + kafkaServer);
        JavaPairInputDStream<String, String> messages = sd.createSparkStreamFromKafka(jssc, "testtopic", kafkaServer);

        System.out.println("LISTENING!!!!!");

        messages.print();

        JavaDStream<String> lines = messages.map(x -> x._2);
        lines.print();


        jssc.start();
        jssc.awaitTermination();
*/    }

    private String generateMessge(int i) {
        JSONObject obj = new JSONObject();
        try {
            obj.put("id", String.valueOf(i));
            obj.put("msg", "test-message" + i);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return obj.toString();
    }

}