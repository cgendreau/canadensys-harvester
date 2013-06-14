package net.canadensys.processing.occurrence.job;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import javax.sql.DataSource;

import net.canadensys.processing.config.ProcessingConfigTest;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes=ProcessingConfigTest.class, loader=AnnotationConfigContextLoader.class)
public class ComputeUniqueValueJobTest {
	
	@Autowired
	private ComputeUniqueValueJob computeUniqueValueJob;
	
    private JdbcTemplate jdbcTemplate;
    
    @Autowired
    public void setDataSource(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }
    
    @Before
    public void init(){
    	jdbcTemplate.update("INSERT INTO occurrence (auto_id,dwcaid,country,locality,sourcefileid) VALUES (1,'1','Mexico','Acapulco','uom-occurrence')");
		jdbcTemplate.update("INSERT INTO occurrence (auto_id,dwcaid,country,locality,sourcefileid) VALUES (2,'2','Australia','Sydney','uoa-occurrence')");
		jdbcTemplate.update("INSERT INTO occurrence (auto_id,dwcaid,country,locality,sourcefileid) VALUES (3,'3','Côte d''Ivoire','Abidjan','uoic-occurrence')");
		jdbcTemplate.update("INSERT INTO occurrence (auto_id,dwcaid,country,locality,sourcefileid) VALUES (4,'4','Australia','Melbourne','uoa-occurrence')");
    }
    
	@Test
	public void testComputeUniqueValues(){
		//create and run the job

		computeUniqueValueJob.doJob();
		
		//validate results
		List<String> countryList = jdbcTemplate.queryForList("SELECT value from unique_values where key='country'",String.class);
		assertTrue(countryList.contains("Mexico") && countryList.contains("Australia") && countryList.contains("Côte d'Ivoire"));
		assertEquals(3, countryList.size());
		
		int australiaCount = jdbcTemplate.queryForObject("SELECT occurrence_count from unique_values where value='Australia'",Integer.class);
		assertEquals(2, australiaCount);
		
		//String coteIvoireUnaccent = jdbcTemplate.queryForObject("SELECT value from unique_values where unaccented_value LIKE 'cote%'",String.class);
		//assertEquals("Côte d'Ivoire", coteIvoireUnaccent);
	}
}
