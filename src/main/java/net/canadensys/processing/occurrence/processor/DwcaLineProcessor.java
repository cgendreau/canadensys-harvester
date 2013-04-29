package net.canadensys.processing.occurrence.processor;

import java.math.BigInteger;
import java.util.Map;

import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;
import net.canadensys.processing.ItemProcessorIF;
import net.canadensys.processing.exception.TaskExecutionException;
import net.canadensys.processing.occurrence.BatchConstant;

import org.apache.log4j.Logger;
import org.hibernate.SQLQuery;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;

/**
 * Processing each line read from a Darwin Core Archive.
 * Attribute a unique id to link the raw and processed model together
 * @author canadenys
 *
 */
public class DwcaLineProcessor implements ItemProcessorIF<OccurrenceRawModel, OccurrenceRawModel>{

	private SessionFactory sessionFactory;
	private StatelessSession session;
	private SQLQuery sqlQuery;
	
	//get log4j handler
	private static final Logger LOGGER = Logger.getLogger(DwcaLineProcessor.class);
	
	@Override
	public void init(){
		session = sessionFactory.openStatelessSession();
		session.beginTransaction();
		sqlQuery = session.createSQLQuery("SELECT nextval('buffer.occurrence_raw_auto_id_seq')");
	}
	
	@Override
	public void destroy(){
		session.getTransaction().commit();
	}
	
	/**
	 * @return same instance of OccurrenceRawModel with modified values
	 */
	@Override
	public OccurrenceRawModel process(OccurrenceRawModel occModel, Map<String,Object> sharedParameters) {
		String sourceFileId = (String)sharedParameters.get(BatchConstant.DWCA_IDENTIFIER_TAG);
        
        if(sourceFileId == null){
			LOGGER.fatal("Misconfigured processor : needs  sourceFileId");
			throw new TaskExecutionException("Misconfigured processor");
		}
		occModel.setSourcefileid(sourceFileId);
		
		BigInteger auto_id = (BigInteger)sqlQuery.uniqueResult();
		occModel.setAuto_id(auto_id.intValue());
		
		//TODO maybe check the uniqueness of occModel.getId()
		return occModel;
	}

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

}
