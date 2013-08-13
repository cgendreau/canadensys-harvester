package net.canadensys.processing.occurrence.job;

import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;

import javax.sql.DataSource;

import net.canadensys.processing.config.ProcessingConfigTest;
import net.canadensys.processing.occurrence.SharedParameterEnum;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

/**
 * Test coverage : 
 * -Move to public schema
 * -Log the import
 * 
 * @author canadensys
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes=ProcessingConfigTest.class, loader=AnnotationConfigContextLoader.class)
public class MoveToPublicSchemaTest {
	@Autowired
	private MoveToPublicSchemaJob moveJob;
	
    private JdbcTemplate jdbcTemplate;
    
    @Autowired
    public void setDataSource(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }
    
    @Before
    public void setupTest(){
    	jdbcTemplate.batchUpdate(new String[]{
    			"DELETE FROM buffer.occurrence",
    			"DELETE FROM buffer.resource_contact",
    			"INSERT INTO buffer.occurrence (auto_id,dwcaid,stateprovince,sourcefileid) VALUES (1,'1','Delaware','qmor-specimens')",
    			"INSERT INTO buffer.occurrence (auto_id,dwcaid,stateprovince,sourcefileid) VALUES (2,'3','Florida','qmor-specimens')",
    			"INSERT INTO buffer.resource_contact (name,dataset_shortname) VALUES ('Louise Cloutier','qmor-specimens')"
    	});
    }
	
	@Test
	public void testMoveToPublicSchema(){
		moveJob.addToSharedParameters(SharedParameterEnum.DATASET_SHORTNAME, "qmor-specimens");
		
		moveJob.doJob();

		String state = jdbcTemplate.queryForObject("SELECT stateprovince FROM occurrence where dwcaid='3'", String.class);
		assertTrue("Florida".equals(state));
		
		String source = jdbcTemplate.queryForObject("SELECT sourcefileid FROM occurrence where dwcaid='1'", String.class);
		assertTrue("qmor-specimens".equals(source));
		
		int count = jdbcTemplate.queryForObject("SELECT count(*) FROM occurrence",BigDecimal.class).intValue();
		assertTrue(new Integer(2).equals(count));
		
		String resource_contact = jdbcTemplate.queryForObject("SELECT name FROM resource_contact where dataset_shortname='qmor-specimens'", String.class);
		assertTrue("Louise Cloutier".equals(resource_contact));
		
		//validate import log
		Integer record_quantity_log = jdbcTemplate.queryForObject("SELECT record_quantity FROM import_log where sourcefileid = 'qmor-specimens'", Integer.class);
		assertTrue(new Integer(2).equals(record_quantity_log));
	}
}
