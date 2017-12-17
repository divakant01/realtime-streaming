package com.dk.elasticsearch.module;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class ESConnect {

	private static ESConnect eSConnect;

	private ESConnect() {

	}

	public static ESConnect getInstance() {

		if (eSConnect == null)
			return new ESConnect();
		else
			return eSConnect;
	}

	@SuppressWarnings("resource")
	public TransportClient getEsConnection() {
		
		Settings settings = Settings.builder().put("client.transport.sniff", true).put("cluster.name", "elasticsearch")
				.build();
		try {
			return new PreBuiltTransportClient(settings)
					.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		throw new IllegalArgumentException("Cannot get Es Connection!");
	}

}
