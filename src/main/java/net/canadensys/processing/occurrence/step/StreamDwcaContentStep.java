package net.canadensys.processing.occurrence.step;

import java.util.Calendar;
import java.util.Map;

import javax.jms.IllegalStateException;

import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;
import net.canadensys.processing.ItemProcessorIF;
import net.canadensys.processing.ItemReaderIF;
import net.canadensys.processing.ItemWriterIF;
import net.canadensys.processing.ProcessingMessageIF;
import net.canadensys.processing.ProcessingStepIF;
import net.canadensys.processing.occurrence.message.ProcessOccurrenceMessage;
import net.canadensys.processing.occurrence.message.SaveRawOccurrenceMessage;

/**
 * Step reading a DarwinCore archive line, process the line, writing the processed lines at a fixed interval as ProcessingMessageIF.
 * NOT thread safe
 * @author canadensys
 *
 */
public class StreamDwcaContentStep implements ProcessingStepIF{
	
	private static final int FLUSH_INTERVAL = 10;
	
	private ItemReaderIF<OccurrenceRawModel> reader;
	private ItemWriterIF<ProcessingMessageIF> writer;
	private ItemProcessorIF<OccurrenceRawModel, OccurrenceRawModel> lineProcessor;
	
	private int numberOfRecords = 0;
	
	@Override
	public void preStep() throws IllegalStateException {
		writer.open();
		lineProcessor.init();
		
	}

	@Override
	public void postStep() {
		writer.close();
		lineProcessor.destroy();
		reader.close();
	}
	
	public void execute(Map<String,Object> sharedParameters){
		//this should be in pre-step but we need the sharedParameters
		reader.open(sharedParameters);

		SaveRawOccurrenceMessage rom = new SaveRawOccurrenceMessage();
		ProcessOccurrenceMessage com = new ProcessOccurrenceMessage();
		
		OccurrenceRawModel currRawModel = reader.read();
		while(currRawModel != null){
			currRawModel = lineProcessor.process(currRawModel, sharedParameters);

			//should be done by ChunkSplitter
			rom.addRawModel(currRawModel);
			com.addRawModel(currRawModel);
			
			currRawModel = reader.read();
			numberOfRecords++;
			
			if(numberOfRecords % FLUSH_INTERVAL == 0){
				writer.write(rom);
				writer.write(com);
				rom = new SaveRawOccurrenceMessage();
				rom.setWhen(Calendar.getInstance().getTime().toString());
				com = new ProcessOccurrenceMessage();
				com.setWhen(Calendar.getInstance().getTime().toString());
			}
		}
		
		//flush remaining content
		if(rom.getRawModelList().size() > 0){
			writer.write(rom);
			writer.write(com);
		}
		
		
		
		
	}
	
	public int getNumberOfRecords(){
		return numberOfRecords;
	}
	
	public void setReader(ItemReaderIF<OccurrenceRawModel> reader) {
		this.reader = reader;
	}
	public void setWriter(ItemWriterIF<ProcessingMessageIF> writer) {
		this.writer = writer;
	}
	public void setDwcaLineProcessor(
			ItemProcessorIF<OccurrenceRawModel, OccurrenceRawModel> lineProcessor) {
		this.lineProcessor = lineProcessor;
	}
}
