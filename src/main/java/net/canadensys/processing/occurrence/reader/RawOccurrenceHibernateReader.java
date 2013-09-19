package net.canadensys.processing.occurrence.reader;

import java.util.Map;

import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;
import net.canadensys.processing.ItemReaderIF;
import net.canadensys.processing.occurrence.SharedParameterEnum;

import org.hibernate.Criteria;
import org.hibernate.ScrollMode;
import org.hibernate.ScrollableResults;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.criterion.Restrictions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

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
	    
	    searchCriteria.add(Restrictions.eq("sourcefileid", "cmmf-specimens"));
		
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
