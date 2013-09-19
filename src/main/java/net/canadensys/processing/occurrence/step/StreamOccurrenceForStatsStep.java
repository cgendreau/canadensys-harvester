package net.canadensys.processing.occurrence.step;

import java.util.Map;

import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;
import net.canadensys.processing.ItemReaderIF;
import net.canadensys.processing.ItemWriterIF;
import net.canadensys.processing.ProcessingStepIF;
import net.canadensys.processing.message.ProcessingMessageIF;
import net.canadensys.processing.occurrence.SharedParameterEnum;
import net.canadensys.processing.occurrence.message.ProcessOccurrenceStatisticsMessage;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Stream database content for statistics processing as ProcessStatisticsOccurrenceMessage.
 * TODO : this class is sending stats message, who should create the message???
 * @author canadensys
 *
 */
public class StreamOccurrenceForStatsStep implements ProcessingStepIF{
	
	private static final int FLUSH_INTERVAL = 100;
	
	@Autowired
	@Qualifier("rawOccurrenceHibernateReader")
	private ItemReaderIF<OccurrenceRawModel> reader;
	
	@Autowired
	@Qualifier("jmsWriter")
	private ItemWriterIF<ProcessingMessageIF> writer;
	
	private int numberOfRecords = 0;
	private Map<SharedParameterEnum,Object> sharedParameters;
	
	@Override
	public void preStep(Map<SharedParameterEnum,Object> sharedParameters) throws IllegalStateException {
		if(writer == null){
			throw new IllegalStateException("No writer defined");
		}
		if(reader == null){
			throw new IllegalStateException("No reader defined");
		}
		this.sharedParameters = sharedParameters;
		reader.openReader(sharedParameters);
		writer.openWriter();
	}

	@Override
	public void postStep() {
		writer.closeWriter();
		reader.closeReader();
	}

	@Override
	public void doStep() {
		ProcessOccurrenceStatisticsMessage psom = new ProcessOccurrenceStatisticsMessage();
		
		long t= System.currentTimeMillis();
		OccurrenceRawModel currRawModel = reader.read();
		while(currRawModel != null){

			//should be done by ChunkSplitter
			psom.addRawModel(currRawModel);

			currRawModel = reader.read();
			numberOfRecords++;
			
			if(numberOfRecords % FLUSH_INTERVAL == 0){
				writer.write(psom);
				psom = new ProcessOccurrenceStatisticsMessage();
			}
		}
		System.out.println("Streaming the database took :" + (System.currentTimeMillis()-t) + " ms");
		
		//flush remaining content
		if(psom.getRawModelList().size() > 0){
			writer.write(psom);
		}
		
		sharedParameters.put(SharedParameterEnum.NUMBER_OF_RECORDS,numberOfRecords);
	}
}
