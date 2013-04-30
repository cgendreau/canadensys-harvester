package net.canadensys.processing;

import javax.jms.IllegalStateException;

/**
 * A step includes a reader, a processor and a writer. Those are not enforced since the could be used in different ways (inheritance, composition, async messages)
 * @author canadensys
 *
 */
public interface ProcessingStepIF {
	
	/**
	 * Check that the step is ready to go.
	 * Initiate inner components
	 * @throws IllegalStateException
	 */
	public void preStep() throws IllegalStateException;
	
	/**
	 * The step is executed.
	 * Clean up phase.
	 */
	public void postStep();

}
