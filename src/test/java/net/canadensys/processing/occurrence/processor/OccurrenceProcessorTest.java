package net.canadensys.processing.occurrence.processor;

import static org.junit.Assert.*;
import net.canadensys.dataportal.occurrence.model.OccurrenceModel;
import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;

import org.junit.Test;


public class OccurrenceProcessorTest {
	
	/**
	 * Test regular processing
	 */
	@Test
	public void testCleaningMechanism(){
		
		OccurrenceProcessor occProcessor = new OccurrenceProcessor();
		
		OccurrenceRawModel rawModel = new OccurrenceRawModel();
		rawModel.setAssociatedmedia("http://www.google.com | http://yahoo.ca");
		rawModel.setCountry("bra");
		rawModel.setScientificname("Carex Linnaeus");
		
		rawModel.setDecimallatitude("10.2");
		rawModel.setDecimallongitude("27.3");
		
		rawModel.setStateprovince("Rio de Janeiro");
		rawModel.setEventdate("2011-12-26");
		
		try {
			OccurrenceModel processedModel = occProcessor.process(rawModel, null);
			assertEquals("http://www.google.com; http://yahoo.ca", processedModel.getAssociatedmedia());
			assertTrue(processedModel.getHasmedia());
			assertEquals("South America", processedModel.getContinent());
			assertEquals("Rio de Janeiro", processedModel.getStateprovince());
			assertNotNull(processedModel.getDecimallatitude());
			assertNotNull(processedModel.getDecimallongitude());
			assertTrue(processedModel.getHascoordinates());
			
			//scientific name
			assertEquals("Carex", processedModel.getScientificname());
			assertEquals("Linnaeus", processedModel.getScientificnameauthorship());
			
			//decade should be set
			assertEquals(2010, processedModel.getDecade().intValue());
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	/**
	 * Test regular processing with verbatim data
	 */
	@Test
	public void testVerbatimProcessing(){
		OccurrenceProcessor occProcessor = new OccurrenceProcessor();
		
		OccurrenceRawModel rawModel = new OccurrenceRawModel();
		rawModel.setVerbatimlatitude("10°N");
		rawModel.setVerbatimlongitude("27°W");
		rawModel.setVerbatimeventdate("2011-12-26");
		
		try {
			OccurrenceModel processedModel = occProcessor.process(rawModel, null);
			
			assertNotNull(processedModel.getDecimallatitude());
			assertNotNull(processedModel.getDecimallongitude());
			assertTrue(processedModel.getHascoordinates());
			
			//decade should be set
			assertEquals(2010, processedModel.getDecade().intValue());
			assertEquals(2011,processedModel.getSyear().intValue());
			assertEquals(12,processedModel.getSmonth().intValue());
			assertEquals(26,processedModel.getSday().intValue());
			
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	/**
	 * Make sure we do not keep an invalid coordinate if the latitude is wrong but not the
	 * longitude
	 */
	@Test
	public void testCleaningBehaviorOnError(){
		OccurrenceProcessor occProcessor = new OccurrenceProcessor();
		
		OccurrenceRawModel rawModel = new OccurrenceRawModel();
		rawModel.setDecimallatitude("10000");
		rawModel.setDecimallongitude("27.3");
		
		try {
			OccurrenceModel processedModel = occProcessor.process(rawModel, null);
			
			assertNull(processedModel.getDecimallatitude());
			assertNull(processedModel.getDecimallongitude());
			assertFalse(processedModel.getHascoordinates());
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	/**
	 * Make sure in case decimalLatitude and decimalLongitude is missing that we can use the
	 * verbatim fields.
	 */
	@Test
	public void testCleaningVerbatimCoordinates(){
		OccurrenceProcessor occProcessor = new OccurrenceProcessor();
		
		OccurrenceRawModel rawModel = new OccurrenceRawModel();
		rawModel.setVerbatimlatitude("85°04'00\"N");
		rawModel.setVerbatimlongitude("9°39'00\"W");
		try {
			OccurrenceModel processedModel = occProcessor.process(rawModel, null);
			
			//assertNotNull(processedModel.getDecimallatitude());
			//assertNotNull(processedModel.getDecimallongitude());
			//assertTrue(processedModel.getHascoordinates());
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}

}
