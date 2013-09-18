package net.canadensys.processing.occurrence.processor;

import java.util.Map;

import net.canadensys.processing.ItemProcessorIF;
import net.canadensys.processing.occurrence.SharedParameterEnum;
import net.canadensys.processing.occurrence.model.OccurrenceQualityReportElement;

import com.sun.xml.internal.ws.util.StringUtils;

/**
 * -WIP this will NOT compile-
 * Processing an OccurrenceRawModel into OccurrenceQualityReportElement.
 * This processor will basically explain what the quality status of each relevant fields.
 * @author canadensys
 *
 */
public class OccurrenceQualityProcessor implements ItemProcessorIF<OccurrenceRawModel, OccurrenceQualityReportElement>{

	//Processors
	private CountryProcessor countryProcessor = new CountryProcessor();
	private CountryContinentProcessor countryContinentProcessor = new CountryContinentProcessor();
	private AbstractDataProcessor latLongProcessor = new DecimalLatLongProcessor("decimallatitude","decimallongitude");
	private DateProcessor dateProcessor = new DateProcessor("eventdate","syear","smonth","sday");
	private AbstractDataProcessor altitudeProcessor = new NumericPairDataProcessor("minimumelevationinmeters","maximumelevationinmeters");
	private DegreeMinuteToDecimalProcessor dmsProcessor = new DegreeMinuteToDecimalProcessor();
	
	@Override
	public OccurrenceQualityReportElement process(OccurrenceRawModel rawModel, Map<SharedParameterEnum,Object> sharedParameters) {
		OccurrenceQualityReportElement occurrenceQualityReportElement = new OccurrenceQualityReportElement();
		if(StringUtils.isNotBlank(rawModel.getCountry())){
			//Country processing
			countryProcessor.process(rawModel, null, null);
			
			if(StringUtils.isNotBlank(rawModel.getStateProvince())){
				//state or province processing
				if(StringUtils.isNotBlank(cleanedModel.getCountry())){
					//&& stateProvinceProcessorMap.get(cleanedModel.getCountry()) != null){
				}
				stateProvinceProcessorMap.get(cleanedModel.getCountry()).processBean(rawModel, cleanedModel, null, null);
			}
			else{
				occurrenceQualityReportElement.addOccurrenceQualityReportElementResult("stateProvince", OccurrenceQualityReportElement.QualityStatusEnum.MISSING);
			}
		}
		else{
			occurrenceQualityReportElement.addOccurrenceQualityReportElementResult("country", OccurrenceQualityReportElement.QualityStatusEnum.MISSING);
		}
		
		return occurrenceQualityReportElement;
	}

}
