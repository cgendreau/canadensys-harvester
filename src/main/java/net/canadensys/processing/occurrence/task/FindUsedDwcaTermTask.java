package net.canadensys.processing.occurrence.task;

import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import net.canadensys.databaseutils.DarwinCoreTermUtils;
import net.canadensys.processing.ItemTaskIF;
import net.canadensys.processing.occurrence.SharedParameterEnum;

import org.gbif.dwc.terms.ConceptTerm;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.dwc.terms.UnknownTerm;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.transaction.annotation.Transactional;

public class FindUsedDwcaTermTask implements ItemTaskIF{
	
	@Autowired
	@Qualifier(value="publicSessionFactory")
	private SessionFactory sessionFactory;

	@Transactional("publicTransactionManager")
	@Override
	public void execute(Map<SharedParameterEnum,Object> sharedParameters){
		Session session = sessionFactory.getCurrentSession();
		
		SQLQuery query = session.createSQLQuery("SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'occurrence_raw' and table_schema = 'public' and data_type <> 'integer'");
		List<String> allColumns = query.list();
		TreeSet<String> sortedColumns = new TreeSet<String>();
		TermFactory termFactory = new TermFactory();
		ConceptTerm conceptTerm;
		String conceptTermStr;
		
		for(String currCol : allColumns){
			//translate reserved words
			conceptTermStr = DarwinCoreTermUtils.untranslate(currCol);
			//make sure this term is valid
			conceptTerm = termFactory.findTerm(conceptTermStr);
			if(!(conceptTerm instanceof UnknownTerm)){
				System.out.println("SELECT auto_id FROM occurrence_raw WHERE " + currCol + " IS NOT NULL AND " + currCol + " <>'' LIMIT 1");
				query = session.createSQLQuery("SELECT auto_id FROM occurrence_raw WHERE " + currCol + " IS NOT NULL AND " + currCol + " <>'' LIMIT 1");
				if(!query.list().isEmpty()){
					sortedColumns.add(conceptTermStr);
				}
			}
		}
		
		System.out.println(sortedColumns);
	}

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}
}
