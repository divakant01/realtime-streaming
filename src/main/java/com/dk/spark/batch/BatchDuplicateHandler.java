package com.dk.spark.batch;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.transport.TransportClient;

import com.dk.elasticsearch.module.ESConnect;

import scala.Tuple2;

public final class BatchDuplicateHandler {
	private static final Pattern SPACE = Pattern.compile(" ");

	@SuppressWarnings({ "resource" })
	public static void main(String[] args) throws Exception {

		//System.setProperty("hadoop.home.dir", "./src/main/resources/hadoop/");

		
		SparkConf sparkConf = new SparkConf().setAppName("BatchReciever");
		sparkConf.setMaster("local[*]");
		//sparkConf.setSparkHome("G:\\jpmc\\spark-2.1.0-bin-hadoop2.7\\");
		
		JavaSparkContext ssc =  new JavaSparkContext(sparkConf);

		Map<String, String> esProperties = new HashMap<>();
		esProperties.put("uniqueIndex", "uniquerec");
		esProperties.put("duplicateIndex", "duplicaterec");
		esProperties.put("uniqueType", "records");

		Broadcast<Map<String, String>> broadcastVar = ssc.broadcast(esProperties);
		
		 JavaRDD<String> lines=ssc.textFile("./src/main/resources/input");
		 JavaRDD<String> words = lines.flatMap(x -> new HashSet<>(Arrays.asList(SPACE.split(x))).iterator());

		 JavaPairRDD<String, Integer> processedIds = words.mapToPair(idValue -> {
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

		processedIds.collect();

	}
}