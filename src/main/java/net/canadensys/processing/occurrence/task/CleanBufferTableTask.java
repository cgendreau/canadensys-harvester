package net.canadensys.processing.occurrence.task;

import java.util.Map;

import net.canadensys.processing.ItemTaskIF;
import net.canadensys.processing.exception.TaskExecutionException;
import net.canadensys.processing.occurrence.SharedParameterEnum;

import org.apache.log4j.Logger;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.transaction.annotation.Transactional;

/**
 * Task to delete all entries from the occurrence and occurrence_raw tables for a specific sourcefileid.
 * @author canadensys
 *
 */
public class CleanBufferTableTask implements ItemTaskIF {
	
	private String datasetShortname = null;
	
	@Autowired
	@Qualifier(value="bufferSessionFactory")
	private SessionFactory sessionFactory;
	
	//get log4j handler
	private static final Logger LOGGER = Logger.getLogger(CleanBufferTableTask.class);
	
	/**
	 * @param sharedParameters get BatchConstant.DWCA_IDENTIFIER_TAG
	 */
	@Transactional("bufferTransactionManager")
	@Override
	public void execute(Map<SharedParameterEnum,Object> sharedParameters){
		datasetShortname = (String)sharedParameters.get(SharedParameterEnum.DATASET_SHORTNAME);
		
		Session session = sessionFactory.getCurrentSession();
		
		if(datasetShortname == null){
			LOGGER.fatal("Misconfigured task : needs  datasetShortname");
			throw new TaskExecutionException("Misconfigured task");
		}
		SQLQuery query = session.createSQLQuery("DELETE FROM buffer.occurrence_raw WHERE sourcefileid=?");
		query.setString(0, datasetShortname);
		query.executeUpdate();
		
		query = session.createSQLQuery("DELETE FROM buffer.occurrence WHERE sourcefileid=?");
		query.setString(0, datasetShortname);
		query.executeUpdate();
	}

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}
}
