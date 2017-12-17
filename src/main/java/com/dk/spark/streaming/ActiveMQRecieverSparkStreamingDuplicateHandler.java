package com.dk.spark.streaming;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.transport.TransportClient;

import com.dk.elasticsearch.module.ESConnect;
import com.dk.mq.processor.ActiveMQReciever;

import scala.Tuple2;

public final class ActiveMQRecieverSparkStreamingDuplicateHandler {
	private static final Pattern SPACE = Pattern.compile(" ");

	@SuppressWarnings({ "resource", "unchecked" })
	public static void main(String[] args) throws Exception {

		System.setProperty("hadoop.home.dir", "./src/main/resources/hadoop/");

		SparkConf sparkConf = new SparkConf().setAppName("ActiveMQReciever");
		sparkConf.setMaster("local[*]");

		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

		Map<String, String> esProperties = new HashMap<>();
		esProperties.put("uniqueIndex", "uniquerec");
		esProperties.put("duplicateIndex", "duplicaterec");
		esProperties.put("uniqueType", "records");

		Broadcast<Map<String, String>> broadcastVar = ssc.sparkContext().broadcast(esProperties);

		JavaReceiverInputDStream<String> lines = ssc
				.receiverStream(new ActiveMQReciever(StorageLevels.MEMORY_AND_DISK_2));

		JavaDStream<String> words = lines.flatMap(x -> new HashSet<>(Arrays.asList(SPACE.split(x))).iterator());

		JavaPairDStream<String, Integer> processedIds = words.mapToPair(idValue -> {
			System.setProperty("es.set.netty.runtime.available.processors", "false");

			Map<String, String> esPropertiesMap = broadcastVar.value();

			String uniqueIndex = esPropertiesMap.get("uniqueIndex");
			String duplicateIndex = esPropertiesMap.get("duplicateIndex");
			String uniqueType = esPropertiesMap.get("uniqueType");

			GetRequest request = new GetRequest().index(uniqueIndex).type(uniqueType).id(idValue);
			Map<String, Object> map = new HashMap<>();
			map.put("date", new Date());
			TransportClient client = ESConnect.getInstance().getEsConnection();

			if (client.get(request).actionGet().isExists()) {
				client.prepareIndex(duplicateIndex, uniqueType, idValue).setSource(map).get();
			} else {
				client.prepareIndex(uniqueIndex, uniqueType, idValue).setSource(map).get();
			}

			return new Tuple2<>(idValue, 1);
		}).reduceByKey((i1, i2) -> (i1 + i2));

		processedIds.print();

		ssc.start();

		ssc.awaitTermination();
	}
}