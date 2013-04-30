package net.canadensys.processing.occurrence.task;

import java.math.BigInteger;
import java.util.Map;

import net.canadensys.processing.ItemTaskIF;
import net.canadensys.processing.exception.TaskExecutionException;
import net.canadensys.processing.occurrence.SharedParameterEnum;

import org.apache.log4j.Logger;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

import com.google.common.util.concurrent.FutureCallback;

/**
 * Task to check and wait for processing completion.
 * Notification will be sent using the FutureCallback object.
 * @author canadensys
 *
 */
public class CheckProcessingCompletenessTask implements ItemTaskIF{

	private static final Logger LOGGER = Logger.getLogger(CheckProcessingCompletenessTask.class);
	
	private SessionFactory sessionFactory;
	private FutureCallback<Void> callback;
	
	/**
	 * @param sharedParameters get BatchConstant.NUMBER_OF_RECORDS and BatchConstant.DWCA_IDENTIFIER_TAG
	 */
	@Override
	public void execute(Map<SharedParameterEnum, Object> sharedParameters) {
		final Integer numberOfRecords = (Integer)sharedParameters.get(SharedParameterEnum.NUMBER_OF_RECORDS);
		final String datasetShortname = (String)sharedParameters.get(SharedParameterEnum.DATASET_SHORTNAME);
		if(numberOfRecords == null || datasetShortname == null || callback == null){
			LOGGER.fatal("Misconfigured task : needs numberOfRecords, datasetShortname and callback");
			throw new TaskExecutionException("Misconfigured task");
		}
		
		Thread checkThread = new Thread(new Runnable() {
			@Override
			public void run() {
				Session session = sessionFactory.openSession();
				SQLQuery query = session.createSQLQuery("SELECT count(*) FROM buffer.occurrence_raw WHERE sourcefileid=?");
				query.setString(0, datasetShortname);
				
				BigInteger currNumberOfResult = (BigInteger)query.uniqueResult();
				while(currNumberOfResult.intValue() < numberOfRecords){
					currNumberOfResult = (BigInteger)query.uniqueResult();
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
						break;
					}
				}
				session.close();
				callback.onSuccess(null);
			}
		});
		checkThread.start();
	}
	
	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	public FutureCallback<Void> getCallback() {
		return callback;
	}

	/**
	 * Set callback method to call when the task identifies that the processing is completed.
	 * @param callback
	 */
	public void setCallback(FutureCallback<Void> callback) {
		this.callback = callback;
	}
}
