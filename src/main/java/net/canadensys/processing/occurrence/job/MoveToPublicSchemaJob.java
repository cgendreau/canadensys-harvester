package net.canadensys.processing.occurrence.job;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;

import net.canadensys.processing.ItemTaskIF;
import net.canadensys.processing.occurrence.SharedParameterEnum;
import net.canadensys.processing.occurrence.task.ComputeGISDataTask;
import net.canadensys.processing.occurrence.task.RecordImportTask;
import net.canadensys.processing.occurrence.task.ReplaceOldOccurrenceTask;

public class MoveToPublicSchemaJob {
	protected Map<SharedParameterEnum,Object> sharedParameters = new HashMap<SharedParameterEnum, Object>();
	
	@Autowired
	private ItemTaskIF computeGISDataTask;
	
	@Autowired
	private ItemTaskIF replaceOldOccurrenceTask;
	
	@Autowired
	private ItemTaskIF recordImportTask;
	
	public void addToSharedParameters(SharedParameterEnum key, Object obj){
		sharedParameters.put(key, obj);
	}
	
	public void doJob(){
		
		computeGISDataTask.execute(sharedParameters);
		replaceOldOccurrenceTask.execute(sharedParameters);
		
		//log the import event
		recordImportTask.execute(sharedParameters);
		
		//SSHTask
	}

	public void setComputeGISDataTask(ComputeGISDataTask computeGISDataTask) {
		this.computeGISDataTask = computeGISDataTask;
	}

	public void setReplaceOldOccurrenceTask(
			ReplaceOldOccurrenceTask replaceOldOccurrenceTask) {
		this.replaceOldOccurrenceTask = replaceOldOccurrenceTask;
	}

	public void setRecordImportTask(RecordImportTask recordImportTask) {
		this.recordImportTask = recordImportTask;
	}

}
