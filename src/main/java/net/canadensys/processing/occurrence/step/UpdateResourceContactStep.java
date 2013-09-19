package net.canadensys.processing.occurrence.step;

import java.util.Map;

import net.canadensys.dataportal.occurrence.model.ResourceContactModel;
import net.canadensys.processing.ItemProcessorIF;
import net.canadensys.processing.ItemReaderIF;
import net.canadensys.processing.ItemWriterIF;
import net.canadensys.processing.ProcessingStepIF;
import net.canadensys.processing.occurrence.SharedParameterEnum;

import org.gbif.metadata.eml.Eml;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * This step should not be used in the regular processing loop. It should only be used when we want to update the resource contact without
 * changing the occurrence records.
 * @author canadensys
 *
 */
public class UpdateResourceContactStep implements ProcessingStepIF{
	
	@Autowired
	@Qualifier("dwcaEmlReader")
	private ItemReaderIF<Eml> reader;
	
	@Autowired
	@Qualifier("resourceContactProcessor")
	private ItemProcessorIF<Eml, ResourceContactModel> resourceContactProcessor;
	
	@Autowired
	@Qualifier("resourceContactWriter")
	private ItemWriterIF<ResourceContactModel> writer;
	
	private Map<SharedParameterEnum,Object> sharedParameters;
	
	@Override
	public void preStep(Map<SharedParameterEnum, Object> sharedParameters){
		if(writer == null){
			throw new IllegalStateException("No writer defined");
		}
		if(resourceContactProcessor == null){
			throw new IllegalStateException("No processor defined");
		}
		if(reader == null){
			throw new IllegalStateException("No reader defined");
		}
		this.sharedParameters = sharedParameters;
		reader.openReader(sharedParameters);
		writer.openWriter();
		resourceContactProcessor.init();
	}

	@Override
	public void postStep() {
		writer.closeWriter();
		resourceContactProcessor.destroy();
		reader.closeReader();
	}

	@Override
	public void doStep() {
		Eml emlModel = reader.read();
		ResourceContactModel resourceContactModel = resourceContactProcessor.process(emlModel, sharedParameters);
		writer.write(resourceContactModel);
	}
}
