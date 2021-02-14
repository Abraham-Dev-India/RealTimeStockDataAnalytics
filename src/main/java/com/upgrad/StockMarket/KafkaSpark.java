package com.upgrad.kafkaspark;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.scala.introspect.OrderingLocator;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;

import org.apache.spark.streaming.Durations;

import scala.Tuple2;
import scala.math.Ordering;
import scala.util.parsing.json.JSONArray;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public final class KafkaSpark implements java.io.Serializable {

	public static void main(String[] args) throws Exception {

		// Function to the get the average(used in finding average closing and
		// opening price
		PairFunction<Tuple2<String, Tuple2<Double, Double>>, String, Double> getAverageByKey = (
				tuple) -> {
			Tuple2<Double, Double> val = tuple._2;
			Double total = val._1;
			Double count = val._2;
			Tuple2<String, Double> averagePair = new Tuple2<String, Double>(
					tuple._1, (total / count));
			return averagePair;
		};

		// Basic check for 3 input arguments
		if (args.length < 3) {
			System.err
					.println("Usage: KafkaSparkDemo <brokers> <groupId> <topics>\n"
							+ "  <brokers> is a list of one or more Kafka brokers\n"
							+ "  <groupId> is a consumer group name to consume from topics\n"
							+ "  <topics> is a list of one or more kafka topics to consume from\n\n");
			System.exit(1);
		}

		// Persisting the input arguments into the variables.
		String brokers = args[0];
		String groupId = args[1];
		String topics = args[2];

		// Set a Unique name for the application.
		// Created context with a 60 seconds (1 minute) batch interval.
		SparkConf sparkConf = new SparkConf().setAppName("KafkaSparkDemo")
				.setMaster("local[*]")
				.set("spark.streaming.kafka.consumer.cache.enabled", "false");
		;
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
				Durations.seconds(60));

		// System.out.println("hi again");
		String checkpointDirectory = "";

		// to remove logs
		jssc.sparkContext().setLogLevel("WARN");

		// Split the topics if multiple values are passed.
		Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));

		// Define a new HashMap for holding the kafka information.
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				JsonDeserializer.class);

		// Create direct kafka stream with brokers and topics/
		// LocationStrategy with prefer consistent allows partitions to be
		// distributed consistently to the spark executors.
		// CosumerStrategy allows to subscribe to the kafka topics.
		// JavaInputDStream is a continuous input stream associated to the
		// source.
		Duration timeout = Duration.ofMillis(2000);
		JavaInputDStream<ConsumerRecord<String, JsonNode>> messages = KafkaUtils
				.createDirectStream(jssc,
						LocationStrategies.PreferConsistent(),
						ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

		// JavaDStream is an internal stream object for processed data.
		// Mapping the incoming JSON from kafka stream to the object.
		JavaDStream<StockData> lines = messages
				.map(new Function<ConsumerRecord<String, JsonNode>, StockData>() {
					private static final long serialVersionUID = 1L;

					// overriding the default call method
					@Override
					public StockData call(
							ConsumerRecord<String, JsonNode> record)
							throws IOException {
						ObjectMapper objectMapper = new ObjectMapper();
						final StockData stockdata = objectMapper.treeToValue(
								record.value(), StockData.class);

						ArrayList<String> jsondump = new ArrayList<String>();

						jsondump.add(record.value().toString());

						try (FileWriter file = new FileWriter(
								"D:/study material/upgrad codes/kafka/kafka-tutorials2/jsonDump.txt",
								true)) {

							file.write(record.value().toString());
						}

						System.out.println(jsondump);

						return objectMapper.treeToValue(record.value(),
								StockData.class);
					}
				});

		//Window size = 10 minutes, Sliding Window Size = 5 minutes
		lines.window(Durations.seconds(600), Durations.seconds(300))
				.foreachRDD(data -> {

					// Creating a tuple from the stream in the format
					// Tuple2<String,Double>,
					// where String -> Symbol and Double -> ClosePrice
						JavaPairRDD<String, Double> closeMapToPair = data
								.mapToPair(new PairFunction<StockData, String, Double>() {
									private static final long serialVersionUID = 1L;

									@Override
									public Tuple2<String, Double> call(
											StockData stockData)
											throws Exception {
										return new Tuple2<String, Double>(
												stockData.getSymbol(),
												stockData.getPriceData()
														.getClose());
									};

								});
						// Creating a tuple from the stream in the format
						// Tuple2<String,Double>,
						// where String -> Symbol and Double -> OpenPrice
						JavaPairRDD<String, Double> openMapToPair = data
								.mapToPair(new PairFunction<StockData, String, Double>() {
									private static final long serialVersionUID = 1L;

									@Override
									public Tuple2<String, Double> call(
											StockData stockData)
											throws Exception {
										return new Tuple2<String, Double>(
												stockData.getSymbol(),
												stockData.getPriceData()
														.getOpen());
									};
								});

						// Creating a tuple from the stream in the format
						// Tuple2<String,Double>,
						// where String -> Symbol and Double -> Absolute Value
						// of Volume
						JavaPairRDD<String, Double> volumePair = data
								.mapToPair(new PairFunction<StockData, String, Double>() {
									private static final long serialVersionUID = 1L;

									@Override
									public Tuple2<String, Double> call(
											StockData stockData)
											throws Exception {
										return new Tuple2<String, Double>(
												stockData.getSymbol(), Math
														.abs(stockData
																.getPriceData()
																.getVolume()));
									};
								});

						// closing price processing
						JavaPairRDD<String, Tuple2<Double, Double>> closePriceValueCount = closeMapToPair
								.mapValues(value -> new Tuple2<Double, Double>(
										value, 1.00));
						JavaPairRDD<String, Tuple2<Double, Double>> closePriceReducedCount = closePriceValueCount
								.reduceByKey((tuple1, tuple2) -> new Tuple2<Double, Double>(
										tuple1._1 + tuple2._1, tuple1._2
												+ tuple2._2));
						JavaPairRDD<String, Double> closeAveragePair = closePriceReducedCount
								.mapToPair(getAverageByKey);

						closeAveragePair.foreach(rdd -> System.out
								.println("average closing price = " + rdd._1()
										+ " " + rdd._2()));

						// opening price processing
						JavaPairRDD<String, Tuple2<Double, Double>> openPriceValueCount = openMapToPair
								.mapValues(value -> new Tuple2<Double, Double>(
										value, 1.00));
						JavaPairRDD<String, Tuple2<Double, Double>> openPriceReducedCount = openPriceValueCount
								.reduceByKey((tuple1, tuple2) -> new Tuple2<Double, Double>(
										tuple1._1 + tuple2._1, tuple1._2
												+ tuple2._2));
						JavaPairRDD<String, Double> openAveragePair = openPriceReducedCount
								.mapToPair(getAverageByKey);

						openAveragePair.foreach(rdd -> System.out
								.println("average opening price = " + rdd._1()
										+ " " + rdd._2()));

						// joining average close price and average open price to
						// form a new RDD JavaPairRDD<String,Double> where,
						// String -> Symbol and Double -> Profit(Average close
						// price - Average Open Price
						JavaPairRDD<String, Tuple2<Double, Double>> combine = closeAveragePair
								.join(openAveragePair);
						JavaPairRDD<String, Double> result = combine
								.mapValues(rdd -> rdd._1() - rdd._2());

						result.foreach(rdd -> System.out.println("profit = "
								+ rdd._1() + " " + rdd._2()));

						try {
							// Forming a new Tuple2<String,Double> where,
							// String -> Symbol and Double -> Max Profit
							Tuple2<String, Double> maxProfit = result
									.max(new TupleComparator());
							System.out.println("The stock with max profit is:"
									+ maxProfit);
						} catch (Exception e) {

						}

						JavaPairRDD<String, Double> totalVolume = volumePair
								.reduceByKey((x, y) -> x + y);
						totalVolume.foreach(a -> System.out.println("Volume is"
								+ a));
						try {

							// Forming a new Tuple2<String,Double> where,
							// String -> Symbol and Double -> Max Volume
							Tuple2<String, Double> maxVolume = totalVolume
									.max(new TupleComparator());
							System.out.println("Stock with max volume is:"
									+ maxVolume);
						} catch (Exception e) {

						}

					});

		// Start the streaming computation
		jssc.start();
		// / Add Await Termination to respond to Ctrl+C and gracefully close
		// Spark Streams
		jssc.awaitTermination();

	}
}