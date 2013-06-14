package net.canadensys.processing.occurrence.job;

import org.springframework.beans.factory.annotation.Autowired;

import net.canadensys.processing.ItemTaskIF;
import net.canadensys.processing.occurrence.task.ComputeUniqueValueTask;

/**
 * Job to compute the unique values and their count from the current content of the database.
 * Never run this job in parallel
 * @author canadensys
 *
 */
public class ComputeUniqueValueJob {

	@Autowired
	private ItemTaskIF computeUniqueValueTask;
	
	public void doJob(){
		computeUniqueValueTask.execute(null);
	}
	
	public void setComputeUniqueValueTask(ComputeUniqueValueTask computeUniqueValueTask) {
		this.computeUniqueValueTask = computeUniqueValueTask;
	}
}
