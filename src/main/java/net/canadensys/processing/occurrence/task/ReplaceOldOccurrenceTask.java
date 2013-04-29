package net.canadensys.processing.occurrence.task;

import java.util.Map;

import net.canadensys.processing.ItemTaskIF;
import net.canadensys.processing.exception.TaskExecutionException;
import net.canadensys.processing.occurrence.BatchConstant;

import org.apache.log4j.Logger;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

/**
 * Task to move all the records for a specific sourcefileid from buffer to public database schema
 * @author canadensys
 *
 */
public class ReplaceOldOccurrenceTask implements ItemTaskIF{
	//get log4j handler
	private static final Logger LOGGER = Logger.getLogger(ReplaceOldOccurrenceTask.class);

	private SessionFactory sessionFactory;
	
	/**
	 * @param sharedParameters get BatchConstant.DWCA_IDENTIFIER_TAG, add BatchConstant.NUMBER_OF_RECORDS
	 */
	@Override
	public void execute(Map<String,Object> sharedParameters){
		Session session = sessionFactory.getCurrentSession();
		
		String sourceFileId = (String)sharedParameters.get(BatchConstant.DWCA_IDENTIFIER_TAG);

		if(sourceFileId == null){
			LOGGER.fatal("Misconfigured task : sourceFileId cannot be null");
			throw new TaskExecutionException("Misconfigured task");
		}

		session.beginTransaction();
		//delete old records
		SQLQuery query = session.createSQLQuery("DELETE FROM occurrence WHERE sourcefileid=?");
		query.setString(0, sourceFileId);
		query.executeUpdate();
		query = session.createSQLQuery("DELETE FROM occurrence_raw WHERE sourcefileid=?");
		query.setString(0, sourceFileId);
		query.executeUpdate();
		
		//copy records from buffer
		query = session.createSQLQuery("INSERT INTO occurrence (SELECT * FROM buffer.occurrence WHERE sourcefileid=?)");
		query.setString(0, sourceFileId);
		int numberOfRecords = query.executeUpdate();
		query = session.createSQLQuery("INSERT INTO occurrence_raw (SELECT * FROM buffer.occurrence_raw WHERE sourcefileid=?)");
		query.setString(0, sourceFileId);
		query.executeUpdate();
		
		//empty buffer schema for this sourcefileid
		query = session.createSQLQuery("DELETE FROM buffer.occurrence WHERE sourcefileid=?");
		query.setString(0, sourceFileId);
		query.executeUpdate();
		query = session.createSQLQuery("DELETE FROM buffer.occurrence_raw WHERE sourcefileid=?");
		query.setString(0, sourceFileId);
		query.executeUpdate();
		session.flush();
		session.getTransaction().commit();
		
		sharedParameters.put(BatchConstant.NUMBER_OF_RECORDS, numberOfRecords);
	}

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}
}
