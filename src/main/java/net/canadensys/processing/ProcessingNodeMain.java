package net.canadensys.processing;

import net.canadensys.processing.config.ProcessingNodeConfig;
import net.canadensys.processing.jms.JMSConsumer;
import net.canadensys.processing.jms.JMSConsumerMessageHandler;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
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
	
	public static void main(String[] args) {
		Options cmdLineOptions = new Options();
		cmdLineOptions.addOption("brokerip", true, "IP address of the ActiveMQ broker");
		CommandLineParser parser = new PosixParser();
		CommandLine cmdLine = null;
		try {
			cmdLine = parser.parse(cmdLineOptions, args);	
		} catch (ParseException e) {
			System.out.println(e.getMessage());
		}
		String newBrokerUrl = null;
		if(cmdLine != null){
			String ipAddress = cmdLine.getOptionValue("brokerip");
			if(StringUtils.isNotBlank(ipAddress)){
				newBrokerUrl = String.format(IP, ipAddress);
			}
		}
		
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(ProcessingNodeConfig.class);
		ProcessingNodeMain processingNodeBean = ctx.getBean(ProcessingNodeMain.class);
		processingNodeBean.initiateJob(newBrokerUrl);
	}
}
