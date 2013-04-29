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
 * Task to delete all entries from the occurrence and occurrence_raw tables for a specific sourcefileid.
 * @author canadensys
 *
 */
public class CleanBufferTableTask implements ItemTaskIF {
	
	private String sourceFileId = null;
	private SessionFactory sessionFactory;
	
	//get log4j handler
	private static final Logger LOGGER = Logger.getLogger(CleanBufferTableTask.class);
	
	/**
	 * @param sharedParameters get BatchConstant.DWCA_IDENTIFIER_TAG
	 */
	@Override
	public void execute(Map<String,Object> sharedParameters){
		 sourceFileId = (String)sharedParameters.get(BatchConstant.DWCA_IDENTIFIER_TAG);
		
		Session session = sessionFactory.getCurrentSession();
		
		if(sourceFileId == null){
			LOGGER.fatal("Misconfigured task : needs  sourceFileId");
			throw new TaskExecutionException("Misconfigured task");
		}
		session.beginTransaction();
		SQLQuery query = session.createSQLQuery("DELETE FROM buffer.occurrence_raw WHERE sourcefileid=?");
		query.setString(0, sourceFileId);
		query.executeUpdate();
		
		query = session.createSQLQuery("DELETE FROM buffer.occurrence WHERE sourcefileid=?");
		query.setString(0, sourceFileId);
		query.executeUpdate();
		session.getTransaction().commit();
	}

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}
}
