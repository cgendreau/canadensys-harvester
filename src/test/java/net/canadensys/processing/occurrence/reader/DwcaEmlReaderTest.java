package net.canadensys.processing.occurrence.reader;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import net.canadensys.processing.ItemReaderIF;
import net.canadensys.processing.occurrence.SharedParameterEnum;

import org.gbif.metadata.eml.Eml;
import org.junit.Test;



public class DwcaEmlReaderTest {
	
	@Test
	public void testEmlRead(){
		Map<SharedParameterEnum,Object> sharedParameters = new HashMap<SharedParameterEnum, Object>();
		sharedParameters.put(SharedParameterEnum.DWCA_PATH,"src/test/resources/dwca-qmor-specimens");
		ItemReaderIF<Eml> emlreader = new DwcaEmlReader();
		emlreader.openReader(sharedParameters);
		Eml eml = emlreader.read();
		
		assertEquals("Louise Cloutier",eml.getContact().getFullName());
	}

}
