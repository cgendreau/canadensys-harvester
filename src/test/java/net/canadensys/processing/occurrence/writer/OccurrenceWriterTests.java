package net.canadensys.processing.occurrence.writer;

import static org.junit.Assert.assertTrue;

import javax.sql.DataSource;

import net.canadensys.dataportal.occurrence.model.OccurrenceModel;
import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Make sure that the different writers work properly
 * @author canadensys
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "/test-harvester.xml" })
public class OccurrenceWriterTests {
	
	private SessionFactory sessionFactory;
	
	private JdbcTemplate jdbcTemplate;
	
	private SessionFactory getSessionFactory(){
		if(sessionFactory == null){
			Configuration configuration = new Configuration();
		    configuration.configure("test_buffer_hibernate.cfg.xml");
		    
		    ServiceRegistry serviceRegistry = new ServiceRegistryBuilder().applySettings(configuration.getProperties()).buildServiceRegistry();        
		    sessionFactory = configuration.buildSessionFactory(serviceRegistry);
		}
	    return sessionFactory;
	}
	    
    @Autowired
    public void setDataSource(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }
	    
	@Test
	public void testRawOccurrenceHibernateWriter(){
	    RawOccurrenceHibernateWriter rawWriter = new RawOccurrenceHibernateWriter();
		rawWriter.setSessionFactory(getSessionFactory());
		
		OccurrenceRawModel rawModel = new OccurrenceRawModel();
		rawModel.setFieldnotes("test note");
		rawModel.setDwcaid("1");
		rawModel.setSourcefileid("sourcefileid");
		rawWriter.write(rawModel);
		
		String state = jdbcTemplate.queryForObject("SELECT fieldnotes FROM buffer.occurrence_raw where dwcaid='1'", String.class);
		assertTrue("test note".equals(state));
	    
	}
	
	@Test
	public void testOccurrenceHibernateWriter(){
	    OccurrenceHibernateWriter rawWriter = new OccurrenceHibernateWriter();
		rawWriter.setSessionFactory(getSessionFactory());
		
		OccurrenceModel rawModel = new OccurrenceModel();
		rawModel.setHabitat("test habitat");
		rawModel.setDwcaid("2");
		rawModel.setSourcefileid("sourcefileid");
		rawWriter.write(rawModel);
		
		String state = jdbcTemplate.queryForObject("SELECT habitat FROM buffer.occurrence where dwcaid='2'", String.class);
		assertTrue("test habitat".equals(state));
	}
}
