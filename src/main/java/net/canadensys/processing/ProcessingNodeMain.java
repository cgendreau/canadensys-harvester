package net.canadensys.processing;

import net.canadensys.processing.config.ProcessingNodeConfig;
import net.canadensys.processing.jms.JMSConsumer;
import net.canadensys.processing.jms.JMSConsumerMessageHandler;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

/**
 * Processing node main class.
 * @author canadensys
 *
 */
@Component
public class ProcessingNodeMain {
	
	private static final String IP = "tcp://%s:61616";
	
	@Autowired
	private JMSConsumer jmsConsumer;
	
	@Autowired
	@Qualifier("insertRawOccurrenceStep")
	private ProcessingStepIF insertRawOccurrenceStep;
	
	@Autowired
	private ProcessingStepIF processInsertOccurrenceStep;
	
	/**
	 * 
	 * @param brokerURL a new broker URL or null to use the default one
	 */
	public void initiateJob(String brokerURL){
		//check if we need to set a new broker URL
		if(StringUtils.isNotBlank(brokerURL)){
			jmsConsumer.setBrokerURL(brokerURL);
		}
		
		System.out.println("Broker location : " + jmsConsumer.getBrokerUrl());
		
		//Declare steps
		jmsConsumer.registerHandler((JMSConsumerMessageHandler)insertRawOccurrenceStep);
		jmsConsumer.registerHandler((JMSConsumerMessageHandler)processInsertOccurrenceStep);
		
		try {
			insertRawOccurrenceStep.preStep(null);
			processInsertOccurrenceStep.preStep(null);
		} catch (IllegalStateException e) {
			e.printStackTrace();
		}
		
		//TODO register postStep calls
		
		jmsConsumer.open();
	}
	
	public static void main(String newBrokerIp) {
		String newBrokerUrl = null;
		if(StringUtils.isNotBlank(newBrokerIp)){
			newBrokerUrl = String.format(IP, newBrokerIp);
		}
		
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(ProcessingNodeConfig.class);
		ProcessingNodeMain processingNodeBean = ctx.getBean(ProcessingNodeMain.class);
		processingNodeBean.initiateJob(newBrokerUrl);
	}
}
