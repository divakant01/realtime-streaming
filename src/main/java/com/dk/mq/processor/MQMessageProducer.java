package com.dk.mq.processor;

import java.util.Random;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MQMessageProducer {
	private static final Logger LOG = LoggerFactory.getLogger(MQMessageProducer.class);

	private MQMessageProducer() {
	}

	private static String generateMessage() {
		int N = 10;
		StringBuilder sb = new StringBuilder();
		Random r = new Random(N);
		for (int i = 0; i < N; i++) {

			sb.append("messsage").append(r.nextInt(N + 1)).append(" ");
		}
		return sb.toString();
	}

	private static void sendMessageToMQ() throws JMSException {

		String url = "tcp://127.0.0.1:61616";

		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
		Destination destination = new ActiveMQQueue("jpmc-messaging");

		Connection connection = connectionFactory.createConnection();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		MessageProducer producer = session.createProducer(destination);
		ActiveMQMessage message = (ActiveMQMessage) session.createTextMessage();

		((TextMessage) message).setText(generateMessage());
		LOG.info("Sending message: " + ((TextMessage) message).getText());

		producer.send(message);

		connection.close();

	}

	public static void main(String[] args) throws JMSException, InterruptedException {
/*		System.out.println(generateMessage());
		for(int i=0;i<20;i++){
			sendMessageToMQ();
			if(i==5)
				Thread.sleep(10000);
		}*/
		sendMessageToMQ();
	}

}