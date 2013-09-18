package net.canadensys.processing.occurrence.step;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.canadensys.processing.ItemProcessorIF;
import net.canadensys.processing.ProcessingStepIF;
import net.canadensys.processing.jms.JMSConsumerMessageHandler;
import net.canadensys.processing.message.ProcessingMessageIF;
import net.canadensys.processing.occurrence.SharedParameterEnum;
import net.canadensys.processing.occurrence.message.ProcessOccurrenceMessage;
import net.canadensys.processing.occurrence.message.ProcessOccurrenceStatisticsMessage;
import net.canadensys.processing.occurrence.model.OccurrenceQualityReportElement;

/**
 * -WIP this will NOT compile-
 * Step taking a ProcessOccurrenceStatisticsMessage from JMS, process the raw occurrence into OccurrenceQualityReportElement
 * 
 * @author canadensys
 *
 */
public class ProcessOccurrenceStatisticsStep implements ProcessingStepIF,JMSConsumerMessageHandler{

	@Autowired
	@Qualifier("occurrenceQualityProcessor")
	private ItemProcessorIF<OccurrenceRawModel, OccurrenceQualityReportElement> processor;
	
	@Override
	public void preStep(Map<SharedParameterEnum,Object> sharedParameters) throws IllegalStateException{
	}

	@Override
	public void postStep() {
	}
	
	@Override
	public Class<?> getMessageClass() {
		return ProcessOccurrenceStatisticsMessage.class;
	}

	@Override
	public void handleMessage(ProcessingMessageIF message) {
		List<OccurrenceRawModel> occRawList = ((ProcessOccurrenceStatisticsMessage)message).getRawModelList();
		//List<OccurrenceModel> occList = new ArrayList<OccurrenceModel>();
		OccurrenceQualityReportElement oqre;
		for(OccurrenceRawModel currRawModel : occRawList){
			oqre = processor.process(currRawModel, null);
		}
	}
	
	/**
	 * No implemented, async step
	 */
	@Override
	public void doStep() {};
	

}
