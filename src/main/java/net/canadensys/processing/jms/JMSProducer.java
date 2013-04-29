package net.canadensys.processing.jms;

import java.io.IOException;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import net.canadensys.processing.ProcessingMessageIF;

import org.apache.activemq.ActiveMQConnectionFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Java Messaging System message producer
 * @author canadensys
 *
 */
public class JMSProducer{

	private String brokerURL;
	
	// Name of the queue we will sent messages into
	private static String queueName = "MyQueue";
	
	private Connection connection;
	private Session session;
	private MessageProducer producer;
	
	//Jackson Mapper to write Java object into JSON
	private ObjectMapper om;
	
	public JMSProducer(String brokerURL){
		this.brokerURL = brokerURL;
	}

	public void init() {
		om = new ObjectMapper();
		ConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
		
		try{
			// Getting JMS connection from the server and starting it
			connection = factory.createConnection();
	
			connection.start();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
	
			Destination destination = session.createQueue(queueName);
			producer = session.createProducer(destination);
		}
		catch(JMSException jEx){
			jEx.printStackTrace();
		}
	}

	public void close() {
		try {
			connection.close();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Send message to the broker
	 * @param element
	 */
	public void send(ProcessingMessageIF element) {
		TextMessage message;
		try {
			message = session.createTextMessage(om.writeValueAsString(element));
			message.setStringProperty("MessageClass", element.getClass().getCanonicalName());
			producer.send(message);
		} catch (JMSException e) {
			e.printStackTrace();
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
