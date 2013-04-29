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
 * Task to compute all GIS related data in our PostGIS enabled table.
 * To ensure maximum performance we run this once at the end of the import
 * @author canadensys
 *
 */
public class ComputeGISDataTask implements ItemTaskIF{
	
	//get log4j handler
	private static final Logger LOGGER = Logger.getLogger(ComputeGISDataTask.class);
		
	private SessionFactory sessionFactory;
	
	/**
	 * @param sharedParameters get BatchConstant.DWCA_IDENTIFIER_TAG
	 */
	@Override
	public void execute(Map<String,Object> sharedParameters){
		String sourceFileId = (String)sharedParameters.get(BatchConstant.DWCA_IDENTIFIER_TAG);
		Session session = sessionFactory.getCurrentSession();
		
		if(sourceFileId == null){
			LOGGER.fatal("Misconfigured task : needs  sourceFileId");
			throw new TaskExecutionException("Misconfigured task");
		}
		session.beginTransaction();
		//update the_geom
		SQLQuery query = session.createSQLQuery("UPDATE buffer.occurrence SET the_geom = st_geometryfromtext('POINT('||decimallongitude||' '|| decimallatitude ||')',4326) " +
				"WHERE sourcefileid=? AND decimallatitude IS NOT NULL AND decimallongitude IS NOT NULL");
		query.setString(0, sourceFileId);
		query.executeUpdate();
		
		//update the_geom_webmercator
		query = session.createSQLQuery("UPDATE buffer.occurrence SET the_geom_webmercator = st_transform_null(the_geom,3857) WHERE sourcefileid=? AND the_geom IS NOT NULL");
		query.setString(0, sourceFileId);
		query.executeUpdate();
		session.flush();
		session.getTransaction().commit();
	}
	
	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

}
