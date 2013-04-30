package net.canadensys.processing.occurrence.step;

import java.util.List;

import javax.jms.IllegalStateException;

import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;
import net.canadensys.processing.ItemWriterIF;
import net.canadensys.processing.ProcessingMessageIF;
import net.canadensys.processing.ProcessingStepIF;
import net.canadensys.processing.jms.JMSConsumerMessageHandler;
import net.canadensys.processing.occurrence.message.SaveRawOccurrenceMessage;

/**
 * Step taking a SaveRawOccurrenceMessage from JMS and writing a Occurrence Raw object list to a writer
 * NOT thread safe
 * @author canadensys
 *
 */
public class InsertRawOccurrenceStep implements ProcessingStepIF,JMSConsumerMessageHandler{
	
	private ItemWriterIF<OccurrenceRawModel> writer;

	@Override
	public void preStep() throws IllegalStateException{
		if(writer == null){
			throw new IllegalStateException("No writer defined");
		}
		writer.open();
	}

	@Override
	public void postStep() {
		writer.close();
	}
	
	@Override
	public Class<?> getMessageClass() {
		return SaveRawOccurrenceMessage.class;
	}

	@Override
	public void handleMessage(ProcessingMessageIF message) {
		List<OccurrenceRawModel> occRawList = ((SaveRawOccurrenceMessage)message).getRawModelList();
		writer.write(occRawList);
	}
	
	public void setWriter(ItemWriterIF<OccurrenceRawModel> writer){
		this.writer = writer;
	}
}
