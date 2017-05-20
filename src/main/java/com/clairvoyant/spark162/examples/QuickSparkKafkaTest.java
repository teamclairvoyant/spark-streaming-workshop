package com.clairvoyant.spark162.examples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;
import org.apache.spark.streaming.kafka.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;


/**
 * Created by vijaydatla on 18/05/17.
 */
public class QuickSparkKafkaTest {


    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        // Create a local StreamingContext with two working thread and batch interval of 1 second
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("KafkaWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));

        Map<String, Integer> topics = new HashMap<String, Integer>();
        topics.put("test", 1);
        JavaPairReceiverInputDStream<String, String> kafkaStream =
                KafkaUtils.createStream(jssc,
                        "52.41.170.89:2181", "console-consumer1", topics);
        // Split each line into words

        JavaDStream<String> lines = kafkaStream.map( Tuple2::_2);
        JavaDStream<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterable<String> call(String x) {
                        return Arrays.asList(x.split(" "));
                    }
                });

        words.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                if (stringJavaRDD.count() > 0L )
                {
                    stringJavaRDD.foreach(

                            new VoidFunction<String>() {
                                public void call(String s) throws Exception {
                                    System.out.println(s);
                                }
                            }

                    );
                }
            }
        });

        // Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                     public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                });
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

// Print the first ten elements of each RDD generated in this DStream to the console

        wordCounts.print();

wordCounts.foreachRDD(
        new org.apache.spark.api.java.function.Function<JavaPairRDD<String, Integer>, Void>() {
            private Connection connect = null;
            private Statement statement = null;
            private PreparedStatement preparedStatement = null;
            private ResultSet resultSet = null;

            @Override
            public Void call(JavaPairRDD<String, Integer> stringIntegerJavaPairRDD) throws Exception {
                long count = stringIntegerJavaPairRDD.count();
              if (count>0) {
                  try {
                      // create a mysql database connection
                      String myDriver = "com.mysql.jdbc.Driver";
                      String myUrl = "jdbc:mysql://kogni-mysql-t2l.ca8crwngvcnr.us-west-2.rds.amazonaws.com/sparkworkshop";
                      Class.forName(myDriver);
                      Connection conn = DriverManager.getConnection(myUrl, "clairvoyant", "clairvoyant");

                      // create a sql date object so we can use it in our INSERT statement

                      // the mysql insert statement
                      String query = " insert into test_data1 (topic, val)"
                              + " values (?, ?)";

                      // create the mysql insert preparedstatement
                      PreparedStatement preparedStmt = conn.prepareStatement(query);
                      preparedStmt.setString(1, "test");
                      preparedStmt.setLong(2, stringIntegerJavaPairRDD.count());


                      // execute the preparedstatement
                      preparedStmt.execute();

                      conn.close();
                  } catch (Exception e) {
                      System.err.println("Got an exception!" + e);
                      System.err.println(e.getMessage());
                  }
              }
                return null;
            }
        }


);


        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate
    }
}
