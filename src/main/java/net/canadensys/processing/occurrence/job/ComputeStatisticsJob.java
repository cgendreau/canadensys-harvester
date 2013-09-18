package net.canadensys.processing.occurrence.job;

import net.canadensys.processing.ProcessingStepIF;

/**
 * -WIP this will NOT compile-
 * 
 * @author canadensys
 *
 */
public class ComputeStatisticsJob {
	
	@Autowired
	private ProcessingStepIF streamOccurrenceForStatsStep;
	
	//send control message done
	public void doJob(){
		streamOccurrenceForStatsStep.execute(null);
	}

}
