package net.canadensys.processing.occurrence.job;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import net.canadensys.processing.ProcessingStepIF;
import net.canadensys.processing.config.ProcessingConfigTest;
import net.canadensys.processing.jms.JMSConsumer;
import net.canadensys.processing.jms.JMSConsumerMessageHandler;
import net.canadensys.processing.occurrence.SharedParameterEnum;
import net.canadensys.processing.occurrence.task.RecordImportTask;
import net.canadensys.processing.occurrence.task.ReplaceOldOccurrenceTask;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.google.common.util.concurrent.FutureCallback;

/**
 * Test coverage : 
 * -Read a DarwinCore archive from a folder
 * -Send the content as JMS messages
 * -Insert raw data in buffer schema
 * -Process data and insert results in buffer schema
 * -Wait for completion
 * -Move to public schema
 * -Log the import
 * 
 * Not cover by this test :
 * -GetResourceInfoTask
 * -ComputeGISDataTask
 * @author canadensys
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes=ProcessingConfigTest.class, loader=AnnotationConfigContextLoader.class)
public class ImportDwcaJobTest implements FutureCallback<Void>{
	
	private static final String TEST_BROKER_URL = "vm://localhost?broker.persistent=false";
	private static AtomicBoolean jobComplete = new AtomicBoolean(false);
	
	@Autowired
	@Qualifier(value="bufferSessionFactory")
	private SessionFactory sessionFactory;
	
	@Autowired
	private ImportDwcaJob importDwcaJob;
	
	@Autowired
	@Qualifier("insertRawOccurrenceStep")
	private JMSConsumerMessageHandler insertRawOccurrenceStep;
	
	@Autowired
	@Qualifier("processInsertOccurrenceStep")
	private JMSConsumerMessageHandler processInsertOccurrenceStep;
	
//	@Autowired
//	private CleanBufferTableTask cleanBufferTableTask;
	
    private JdbcTemplate jdbcTemplate;
    
    @Autowired
    public void setDataSource(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }
	
	@Test
	public void testImport(){
		//.setAllowDatasetShortnameExtraction(true);
		
		//CleanBufferTableTask cleanBufferTableTask = new CleanBufferTableTask();
		//cleanBufferTableTask.setSessionFactory(sessionFactory);
		
		
		//add a local consumer to test the entire loop
		setupTestConsumer();
						
		importDwcaJob.addToSharedParameters(SharedParameterEnum.DWCA_PATH, "src/test/resources/dwca-qmor-specimens");
		importDwcaJob.addToSharedParameters(SharedParameterEnum.DATASET_SHORTNAME, "qmor-specimens");
		
		importDwcaJob.doJob(this);
		synchronized (jobComplete) {
			try {
				jobComplete.wait();
				//validate content of the database
				if(jobComplete.get()){
					
					String state = jdbcTemplate.queryForObject("SELECT stateprovince FROM buffer.occurrence where dwcaid='3'", String.class);
					assertTrue("Florida".equals(state));
					
					String source = jdbcTemplate.queryForObject("SELECT sourcefileid FROM buffer.occurrence where dwcaid='1'", String.class);
					assertTrue("qmor-specimens".equals(source));
					
					int count = jdbcTemplate.queryForObject("SELECT count(*) FROM buffer.occurrence",BigDecimal.class).intValue();
					assertTrue(new Integer(11).equals(count));
				}
				else{
					fail();
				}
			} catch (InterruptedException e) {
				fail();
			}
		}
	}
	
	/**
	 * This consumer will write to the database specified by the sessionFactory
	 * @param sessionFactory
	 */
	private void setupTestConsumer(){
		JMSConsumer reader = new JMSConsumer(TEST_BROKER_URL);
		reader.registerHandler(insertRawOccurrenceStep);
		reader.registerHandler(processInsertOccurrenceStep);
		
		try {
			((ProcessingStepIF)insertRawOccurrenceStep).preStep(null);
			((ProcessingStepIF)processInsertOccurrenceStep).preStep(null);
		} catch (IllegalStateException e) {
			e.printStackTrace();
		}
		
		reader.open();
	}
	
	/**
	 * This test depends on the testImport test
	 */
	//@Test
	public void testMoveToPublicSchema(){
		//should use dependency injection
	    Configuration configuration = new Configuration();
	    configuration.configure("test-public-hibernate.cfg.xml");
	    
	    ServiceRegistry serviceRegistry = new ServiceRegistryBuilder().applySettings(configuration.getProperties()).buildServiceRegistry();        
	    SessionFactory sessionFactory = configuration.buildSessionFactory(serviceRegistry);
	    
		MoveToPublicSchemaJob moveToPublicSchemaJob = new MoveToPublicSchemaJob();
		moveToPublicSchemaJob.addToSharedParameters(SharedParameterEnum.DATASET_SHORTNAME, "qmor-specimens");
		
		ReplaceOldOccurrenceTask replaceOld = new ReplaceOldOccurrenceTask();
		replaceOld.setSessionFactory(sessionFactory);
		
		RecordImportTask recordImportTask = new RecordImportTask();
		recordImportTask.setSessionFactory(sessionFactory);
		
		//moveToPublicSchemaJob.setComputeGISDataTask(new MockComputeGISDataTask());
		moveToPublicSchemaJob.setReplaceOldOccurrenceTask(replaceOld);
		moveToPublicSchemaJob.setRecordImportTask(recordImportTask);
		moveToPublicSchemaJob.doJob();
		
		String state = jdbcTemplate.queryForObject("SELECT stateprovince FROM occurrence where dwcaid='3'", String.class);
		assertTrue("Florida".equals(state));
		
		String source = jdbcTemplate.queryForObject("SELECT sourcefileid FROM occurrence where dwcaid='1'", String.class);
		assertTrue("qmor-specimens".equals(source));
		
		int count = jdbcTemplate.queryForObject("SELECT count(*) FROM occurrence",BigDecimal.class).intValue();
		assertTrue(new Integer(11).equals(count));
		
		//validate import log
		Integer record_quantity_log = jdbcTemplate.queryForObject("SELECT record_quantity FROM import_log where sourcefileid = 'qmor-specimens'", Integer.class);
		assertTrue(new Integer(11).equals(record_quantity_log));
	}
	
	@Override
	public void onSuccess(Void arg0) {
		synchronized (jobComplete) {
			jobComplete.set(true);
			jobComplete.notifyAll();
		}
	}
	@Override
	public void onFailure(Throwable arg0) {
		fail();
	}
}
