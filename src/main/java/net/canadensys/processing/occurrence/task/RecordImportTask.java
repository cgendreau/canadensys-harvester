package net.canadensys.processing.occurrence.task;

import java.util.Date;
import java.util.Map;

import net.canadensys.processing.ItemTaskIF;
import net.canadensys.processing.exception.TaskExecutionException;
import net.canadensys.processing.occurrence.BatchConstant;
import net.canadensys.processing.occurrence.model.ImportLogModel;

import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

/**
 * Task to record(save) the import for traceability
 * @author canadensys
 *
 */
public class RecordImportTask implements ItemTaskIF{
	private static final String CURRENT_USER = System.getProperty("user.name");
	private SessionFactory sessionFactory;
	
	//get log4j handler
	private static final Logger LOGGER = Logger.getLogger(RecordImportTask.class);
	
	@Override
	public void execute(Map<String,Object> sharedParameters){
		Session session = sessionFactory.getCurrentSession();
		ImportLogModel importLogModel = new ImportLogModel();
		String sourceFileId = (String)sharedParameters.get(BatchConstant.DWCA_IDENTIFIER_TAG);
		Integer numberOfRecords = (Integer)sharedParameters.get(BatchConstant.NUMBER_OF_RECORDS);
		
		if(sourceFileId == null || numberOfRecords == null){
			LOGGER.fatal("Misconfigured task : needs  sourceFileId and numberOfRecords");
			throw new TaskExecutionException("Misconfigured task");
		}
		importLogModel.setSourcefileid(sourceFileId);
		importLogModel.setRecord_quantity(numberOfRecords);
		importLogModel.setUpdated_by(CURRENT_USER);
		importLogModel.setEvent_end_date_time(new Date());
		session.beginTransaction();
		session.save(importLogModel);
		session.getTransaction().commit();
	}
	
	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}
}
