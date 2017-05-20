package com.clairvoyant.spark162.examples;

import com.google.common.base.Optional;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by vijaydatla on 18/05/17.
 */
public class UpdateStateByKeyKafka {


    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        // Create a local StreamingContext with two working thread and batch interval of 1 second


        Function0<JavaStreamingContext> createContextFunc = new Function0<JavaStreamingContext>() {
            public JavaStreamingContext call() throws Exception {
                return createContextFunc();
            }
        };

        JavaStreamingContext ssc =
                JavaStreamingContext.getOrCreate("~/Desktop/checkpoint10", createContextFunc);



        ssc.start();              // Start the computation
        ssc.awaitTermination();   // Wait for the computation to terminate
    }


    public static JavaStreamingContext createContextFunc()
    {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(3));
        jssc.checkpoint("~/Desktop/checkpoint10");

        Map<String, Integer> topics = new HashMap<String, Integer>();
        topics.put("myNewTopic", 1);
        JavaPairReceiverInputDStream<String, String> kafkaStream =
                KafkaUtils.createStream(jssc,
                        "52.41.170.89:2181", "console-consumer1", topics);
        // Split each line into words

        JavaDStream<String> lines = kafkaStream.map(Tuple2::_2);


        // Split each line into words
        JavaDStream<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterable<String> call(String x) {
                        return Arrays.asList(x.split(" "));
                    }
                });

       /* words.foreachRDD(new VoidFunction<JavaRDD<String>>() {
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
        });*/

        // Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) {
//                        return new Tuple2<String, Integer>(s, 1);
                        return new Tuple2<String, Integer>("Persons", 1);

                    }

                });
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

        // Reduce function adding two integers, defined separately for clarity
        Function2<Integer, Integer, Integer> reduceFunc = new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        };

        // Reduce last 30 seconds of data, every 10 seconds
        JavaPairDStream<String, Integer> windowedWordCounts = pairs.reduceByKeyAndWindow(reduceFunc, Durations.seconds(12), Durations.seconds(6));


        //System.out.println("Windowed WordCounts");
        System.out.println("Windowed WordCounts");

        windowedWordCounts.print();
        //pairs.print();

        // Maintain state as new data comes in..

        Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
                new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
                    public Optional<Integer> call(List<Integer> values, Optional<Integer> state) {
                        Integer newSum = state.or(0);

               //         System.out.println(values);
                        for(int i : values)
                        {
                            newSum += i;
                        }
                        return Optional.of(newSum);
                    }
                };

        JavaPairDStream<String, Integer> runningCounts =
                pairs.updateStateByKey(updateFunction);

        System.out.println("Running WordCounts");
        runningCounts.print();
        return jssc;
    }

}
