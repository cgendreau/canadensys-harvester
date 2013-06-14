package net.canadensys.processing.config;

import java.util.Properties;

import javax.sql.DataSource;

import net.canadensys.dataportal.occurrence.model.OccurrenceModel;
import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;
import net.canadensys.processing.ItemProcessorIF;
import net.canadensys.processing.ItemReaderIF;
import net.canadensys.processing.ItemTaskIF;
import net.canadensys.processing.ItemWriterIF;
import net.canadensys.processing.ProcessingStepIF;
import net.canadensys.processing.jms.JMSWriter;
import net.canadensys.processing.occurrence.job.ComputeUniqueValueJob;
import net.canadensys.processing.occurrence.job.ImportDwcaJob;
import net.canadensys.processing.occurrence.job.MoveToPublicSchemaJob;
import net.canadensys.processing.occurrence.mock.MockComputeGISDataTask;
import net.canadensys.processing.occurrence.model.ImportLogModel;
import net.canadensys.processing.occurrence.processor.DwcaLineProcessor;
import net.canadensys.processing.occurrence.processor.OccurrenceProcessor;
import net.canadensys.processing.occurrence.reader.DwcaItemReader;
import net.canadensys.processing.occurrence.step.InsertRawOccurrenceStep;
import net.canadensys.processing.occurrence.step.ProcessInsertOccurrenceStep;
import net.canadensys.processing.occurrence.step.StreamDwcaContentStep;
import net.canadensys.processing.occurrence.task.CheckProcessingCompletenessTask;
import net.canadensys.processing.occurrence.task.CleanBufferTableTask;
import net.canadensys.processing.occurrence.task.ComputeUniqueValueTask;
import net.canadensys.processing.occurrence.task.PrepareDwcaTask;
import net.canadensys.processing.occurrence.task.RecordImportTask;
import net.canadensys.processing.occurrence.task.ReplaceOldOccurrenceTask;
import net.canadensys.processing.occurrence.view.HarvesterViewModel;
import net.canadensys.processing.occurrence.writer.OccurrenceHibernateWriter;
import net.canadensys.processing.occurrence.writer.RawOccurrenceHibernateWriter;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.orm.hibernate4.HibernateTransactionManager;
import org.springframework.orm.hibernate4.LocalSessionFactoryBean;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@ComponentScan(basePackages ="net.canadensys.processing.occurrence",
	excludeFilters = { @Filter(type=FilterType.ASSIGNABLE_TYPE, value=ProcessingConfig.class) })
@EnableTransactionManagement
public class ProcessingConfigTest {
	
    @Bean
    public static PropertyPlaceholderConfigurer properties(){
    	PropertyPlaceholderConfigurer ppc = new PropertyPlaceholderConfigurer();
    	ClassPathResource[] resources = new ClassPathResource[]
    			{ new ClassPathResource( "test-harvester-config.properties" ) };
    	ppc.setLocations( resources );
    	return ppc;
    }
    
    @Value("${database.url}")
    private String dbUrl;
    @Value( "${database.driver}" )
    private String dbDriverClassName;
    
    @Value( "${hibernate.dialect}" )
    private String hibernateDialect;
    @Value( "${hibernate.show_sql}" )
    private String hibernateShowSql;
    @Value( "${hibernate.buffer_schema}" )
    private String hibernateBufferSchema;
    
    @Value( "${occurrence.idGenerationSQL}" )
    private String idGenerationSQL;
    
    @Value("${jms.broker_url}")
    private String jmsBrokerUrl;
    
    @Bean(name="datasource")
    public DataSource dataSource() {
		return new EmbeddedDatabaseBuilder()
			.setType(EmbeddedDatabaseType.H2)
		    .addScript("classpath:createTablesPublicSchema-batch.sql")
		    .addScript("classpath:createTablesBufferSchema-batch.sql")
		    .addScript("classpath:createTablesBatchSpecific.sql")
		    .build();
    }
    
    @Bean(name="bufferSessionFactory")
    public LocalSessionFactoryBean bufferSessionFactory() {
    	LocalSessionFactoryBean sb = new LocalSessionFactoryBean(); 
    	sb.setDataSource(dataSource()); 
    	sb.setAnnotatedClasses(new Class[]{OccurrenceRawModel.class,
    			OccurrenceModel.class,
    			ImportLogModel.class});

		Properties hibernateProperties = new Properties();
		hibernateProperties.setProperty("hibernate.dialect", hibernateDialect);
		hibernateProperties.setProperty("hibernate.show_sql", hibernateShowSql);
		hibernateProperties.setProperty("hibernate.default_schema", hibernateBufferSchema);
		hibernateProperties.setProperty("javax.persistence.validation.mode", "none");
    	sb.setHibernateProperties(hibernateProperties);
    	return sb;
    }
    
    @Bean(name="publicSessionFactory")
    public LocalSessionFactoryBean publicSessionFactory() {
    	LocalSessionFactoryBean sb = new LocalSessionFactoryBean(); 
    	sb.setDataSource(dataSource()); 
    	sb.setAnnotatedClasses(new Class[]{OccurrenceRawModel.class,
    			OccurrenceModel.class,
    			ImportLogModel.class});

		Properties hibernateProperties = new Properties();
		hibernateProperties.setProperty("hibernate.dialect", hibernateDialect);
		hibernateProperties.setProperty("hibernate.show_sql", hibernateShowSql);
		hibernateProperties.setProperty("javax.persistence.validation.mode", "none");
    	sb.setHibernateProperties(hibernateProperties);
    	return sb;
    }
    
    @Bean(name="bufferTransactionManager")
    public HibernateTransactionManager hibernateTransactionManager(){
    	HibernateTransactionManager htmgr = new HibernateTransactionManager();
		htmgr.setSessionFactory(bufferSessionFactory().getObject());
    	return htmgr;
    }
    
    @Bean(name="publicTransactionManager")
    public HibernateTransactionManager publicHibernateTransactionManager(){
    	HibernateTransactionManager htmgr = new HibernateTransactionManager();
		htmgr.setSessionFactory(publicSessionFactory().getObject());
    	return htmgr;
    }
    
    //---VIEW MODEL---
	@Bean
	public HarvesterViewModel harvesterViewModel(){
		return null;
	}
	
    //---JOB---
	@Bean
	public ImportDwcaJob importDwcaJob(){
		return new ImportDwcaJob();
	}
	@Bean
	public MoveToPublicSchemaJob moveToPublicSchemaJob(){
		return new MoveToPublicSchemaJob();
	}
	@Bean
	public ComputeUniqueValueJob computeUniqueValueJob(){
		return new ComputeUniqueValueJob();
	}
	
	//---STEP---
	@Bean(name="streamDwcaContentStep")
	public ProcessingStepIF StreamDwcaContentStep(){
		return new StreamDwcaContentStep();
	}
	@Bean(name="insertRawOccurrenceStep")
	public ProcessingStepIF insertRawOccurrenceStep(){
		return new InsertRawOccurrenceStep();
	}
	
	@Bean(name="processInsertOccurrenceStep")
	public ProcessingStepIF processInsertOccurrenceStep(){
		return new ProcessInsertOccurrenceStep();
	}
	
	//---TASK wiring---
	
	@Bean
	public ItemTaskIF prepareDwcaTask(){
		return new PrepareDwcaTask();
	}
	
	@Bean
	public ItemTaskIF cleanBufferTableTask(){
		return new CleanBufferTableTask();
	}
	
	@Bean
	public ItemTaskIF computeGISDataTask(){
		return new MockComputeGISDataTask();
	}
	
	@Bean
	public ItemTaskIF checkProcessingCompletenessTask(){
		return new CheckProcessingCompletenessTask();
	}
	
	@Bean
	public ItemTaskIF getResourceInfoTask(){
		return null;
	}
	
	@Bean
	public ItemTaskIF computeUniqueValueTask(){
		return new ComputeUniqueValueTask();
	}
	
	@Bean
	public ItemTaskIF replaceOldOccurrenceTask(){
		return new ReplaceOldOccurrenceTask();
	}
	@Bean
	public ItemTaskIF recordImportTask(){
		return new RecordImportTask();
	}
	
	//---PROCESSOR wiring---
	@Bean(name="lineProcessor")
	public ItemProcessorIF<OccurrenceRawModel, OccurrenceRawModel> lineProcessor(){
		DwcaLineProcessor dwcaLineProcessor = new DwcaLineProcessor();
		dwcaLineProcessor.setIdGenerationSQL(idGenerationSQL);
		return dwcaLineProcessor;
	}
	
	@Bean(name="occurrenceProcessor")
	public ItemProcessorIF<OccurrenceRawModel, OccurrenceModel> occurrenceProcessor(){
		return new OccurrenceProcessor();
	}
	
	//---READER wiring---
	@Bean
	public ItemReaderIF<OccurrenceRawModel> dwcaItemReader(){
		return new DwcaItemReader();
	}
	
	//---WRITER wiring---
	@Bean(name="rawOccurrenceWriter")
	public ItemWriterIF<OccurrenceRawModel> rawOccurrenceWriter(){
		return new RawOccurrenceHibernateWriter();
	}
	
	@Bean(name="occurrenceWriter")
	public ItemWriterIF<OccurrenceModel> occurrenceWriter(){
		return new OccurrenceHibernateWriter();
	}
	
	@Bean
	public JMSWriter jmsWriter(){
		return new JMSWriter(jmsBrokerUrl);
	}
	
}
