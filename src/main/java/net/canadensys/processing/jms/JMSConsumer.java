package net.canadensys.processing.jms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import net.canadensys.processing.ProcessingMessageIF;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.log4j.BasicConfigurator;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Java Messaging System message consumer.
 * 
 * @author canadensys
 *
 */
public class JMSConsumer{
	
	public String brokerURL;

	// Name of the queue we will receive messages from
	private static String queueName = "MyQueue";
	
	private Connection connection;
	private MessageConsumer consumer;
	
	private List<JMSConsumerMessageHandler> regiteredHandlers;
	
	//Jackson Mapper to map JSON into Java object
	private ObjectMapper om;
	
	public JMSConsumer(String brokerURL){
		this.brokerURL = brokerURL;
		regiteredHandlers = new ArrayList<JMSConsumerMessageHandler>();
	}
	
	/**
	 * Register a handler to notify when we receive a message
	 * @param handler
	 */
	public void registerHandler(JMSConsumerMessageHandler handler){
		regiteredHandlers.add(handler);
	}
	
	public void open() {
		om = new ObjectMapper();
		BasicConfigurator.configure();
		// Getting JMS connection from the server
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerURL);
		
		try{
			connection = connectionFactory.createConnection();
			connection.start();
	
			// Creating session for sending messages
			Session session = connection.createSession(false,
					Session.AUTO_ACKNOWLEDGE);
	
			// Getting the queue
			Queue queue = session.createQueue(queueName);
	
			// MessageConsumer is used for receiving (consuming) messages
			consumer = session.createConsumer(queue);
			consumer.setMessageListener(new JMSMessageListener());
		}
		catch(JMSException jmsEx){
			jmsEx.printStackTrace();
		}
	}

	public void close() {
		try {
			connection.close();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	
	private class JMSMessageListener implements MessageListener{
		@Override
		public void onMessage(Message message) {
			// There are many types of Message and TextMessage
			// is just one of them. Producer sent us a TextMessage
			// so we must cast to it to get access to its .getText()
			// method.
			if (message instanceof TextMessage) {
				TextMessage msg = (TextMessage) message;
				try {
					Class<?> msgClass = Class.forName(ObjectUtils.defaultIfNull(msg.getStringProperty("MessageClass"), Object.class.getCanonicalName()));
					//validate if we can instantiate
					for(JMSConsumerMessageHandler currMsgHandler : regiteredHandlers){
						if(currMsgHandler.getMessageClass().equals(msgClass)){
							ProcessingMessageIF chunk = (ProcessingMessageIF)om.readValue(msg.getText(), msgClass);
							currMsgHandler.handleMessage(chunk);
							break;
						}

						//TODO : raise error if no handler can process it
					}
				} catch (JMSException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} catch (JsonParseException e) {
					e.printStackTrace();
				} catch (JsonMappingException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
