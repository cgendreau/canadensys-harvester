package net.canadensys.processing.occurrence.reader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.Criteria;
import org.hibernate.ScrollMode;
import org.hibernate.ScrollableResults;

import net.canadensys.databaseutils.ScrollableResultsIteratorWrapper;
import net.canadensys.dataportal.occurrence.model.OccurrenceModel;
import net.canadensys.processing.ItemMapperIF;
import net.canadensys.processing.ItemReaderIF;
import net.canadensys.processing.occurrence.SharedParameterEnum;
import net.canadensys.processing.occurrence.mapper.OccurrenceMapper;
import net.canadensys.processing.occurrence.writer.Autowired;
import net.canadensys.processing.occurrence.writer.Qualifier;
import net.canadensys.processing.occurrence.writer.SessionFactory;
import net.canadensys.processing.occurrence.writer.StatelessSession;

/**
 * -WIP this will NOT compile-
 * Reading raw occurrence (OccurrenceRawModel) from database.
 * @author canadensys
 *
 */
public class RawOccurrenceHibernateReader implements ItemReaderIF<OccurrenceRawModel>{
	
	private static final int DEFAULT_FLUSH_LIMIT = 1000;
	
	@Autowired
	@Qualifier(value="publicSessionFactory")
	private SessionFactory sessionFactory;
	
	private StatelessSession session;
	private ScrollableResults sr;
	
	@Override
	public void openReader(Map<SharedParameterEnum,Object> sharedParameters){
	    session = sessionFactory.openStatelessSession();
	    Criteria searchCriteria = session.createCriteria(OccurrenceRawModel.class);
		
	    searchCriteria.setFetchSize(DEFAULT_FLUSH_LIMIT);  // experiment with this to optimize performance vs. memory
	  	sr = searchCriteria.scroll(ScrollMode.FORWARD_ONLY);
	}

	@Override
	public void closeReader() {
		sr.close();
		session.close();
	}
	
	@Override
	public OccurrenceRawModel read(){
		if(sr.next()){
			return (OccurrenceRawModel)sr.get()[0];
		}
		return null;
	}
	
	public void setSessionFactory(SessionFactory sessionFactory){
		this.sessionFactory = sessionFactory;
	}

}
