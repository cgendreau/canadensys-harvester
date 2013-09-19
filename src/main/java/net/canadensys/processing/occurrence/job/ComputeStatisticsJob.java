package net.canadensys.processing.occurrence.job;

import java.util.HashMap;
import java.util.Map;

import net.canadensys.processing.ProcessingStepIF;
import net.canadensys.processing.occurrence.SharedParameterEnum;

import org.springframework.beans.factory.annotation.Autowired;

/**
 * -WIP this will NOT compile-
 * 
 * @author canadensys
 *
 */
public class ComputeStatisticsJob {
	
	protected Map<SharedParameterEnum,Object> sharedParameters = new HashMap<SharedParameterEnum, Object>();
	
	@Autowired
	private ProcessingStepIF streamOccurrenceForStatsStep;
	
	//send control message done
	public void doJob(){
		streamOccurrenceForStatsStep.preStep(sharedParameters);
		streamOccurrenceForStatsStep.doStep();
		streamOccurrenceForStatsStep.postStep();
	}

}
