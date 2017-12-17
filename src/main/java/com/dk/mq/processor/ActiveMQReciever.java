package com.dk.mq.processor;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

@SuppressWarnings("rawtypes")
public class ActiveMQReciever extends Receiver {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2985344461761403782L;
	private Connection connection = null;;

	public ActiveMQReciever(StorageLevel storageLevel) {
		super(storageLevel);
	}

	@Override
	public void onStart() {
		new Thread(new Runnable(){
	
				@SuppressWarnings("unchecked")
				@Override
				public void run() {
					  String url = "tcp://localhost:61616";
					  
				        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
				        Destination destination = new ActiveMQQueue("jpmc-messaging");

						try {
							connection = connectionFactory.createConnection();
							 connection.start();

						        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
						        MessageConsumer consumer = session.createConsumer(destination);
					            System.out.println("Waiting for message.");
					            ActiveMQTextMessage message = (ActiveMQTextMessage)  consumer.receive();
					            store(message.getText());
					    		if(connection!=null)
					    			connection.close();
					    		restart("After Processing");
						} catch (Throwable  e) {
							restart("After Exception" + e);
							//e.printStackTrace();
						}					
				}
		       
		}).start();
	}

	@Override
	public void onStop() {

	}
}