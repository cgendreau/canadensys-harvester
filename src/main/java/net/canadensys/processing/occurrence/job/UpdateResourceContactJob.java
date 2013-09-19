package net.canadensys.processing.occurrence.job;

import java.util.HashMap;
import java.util.Map;

import net.canadensys.processing.ItemTaskIF;
import net.canadensys.processing.ProcessingStepIF;
import net.canadensys.processing.occurrence.SharedParameterEnum;

import org.springframework.beans.factory.annotation.Autowired;

/**
 * 
 * @author canadensys
 *
 */
public class UpdateResourceContactJob {

	protected Map<SharedParameterEnum,Object> sharedParameters = new HashMap<SharedParameterEnum, Object>();
	
	//Task and step
	@Autowired
	private ItemTaskIF getResourceInfoTask;
	
	@Autowired
	private ItemTaskIF prepareDwcaTask;
	
	@Autowired
	private ProcessingStepIF updateResourceContactStep;
	
	public void clearSharedParameters(){
		sharedParameters.clear();
	}
	public void addToSharedParameters(SharedParameterEnum key, Object obj){
		sharedParameters.put(key, obj);
	}
	
	public Object getSharedParameter(SharedParameterEnum key){
		return sharedParameters.get(key);
	}
	
	public void doJob(){
		getResourceInfoTask.execute(sharedParameters);
		prepareDwcaTask.execute(sharedParameters);
	
		updateResourceContactStep.preStep(sharedParameters);
		updateResourceContactStep.doStep();
		updateResourceContactStep.postStep();
	}
}
