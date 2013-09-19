package net.canadensys.processing.occurrence.processor;

import java.util.HashMap;
import java.util.Map;

import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;
import net.canadensys.processing.ItemProcessorIF;
import net.canadensys.processing.occurrence.SharedParameterEnum;
import net.canadensys.processing.occurrence.model.OccurrenceQualityReportElement;
import net.canadensys.processor.ProcessingResult;
import net.canadensys.processor.geography.CountryProcessor;
import net.canadensys.processor.geography.StateProvinceProcessor;
import net.canadensys.vocabulary.stateprovince.BEProvince;
import net.canadensys.vocabulary.stateprovince.CAProvince;
import net.canadensys.vocabulary.stateprovince.StateProvinceEnum;

import org.apache.commons.lang3.StringUtils;
import org.gbif.api.model.vocabulary.Country;
import org.gbif.dwc.terms.DwcTerm;


/**
 * Processing an OccurrenceRawModel into OccurrenceQualityReportElement.
 * This processor will basically explain what the quality status of each relevant fields.
 * @author canadensys
 *
 */
public class OccurrenceQualityProcessor implements ItemProcessorIF<OccurrenceRawModel, OccurrenceQualityReportElement>{
	
	private String COUNTRY = DwcTerm.country.simpleName();
	private String STATE_PROVINCE = DwcTerm.stateProvince.simpleName();

	//Processors
	private CountryProcessor countryProcessor = new CountryProcessor();
	//private CountryContinentProcessor countryContinentProcessor = new CountryContinentProcessor();
	//private AbstractDataProcessor latLongProcessor = new DecimalLatLongProcessor("decimallatitude","decimallongitude");
	//private DateProcessor dateProcessor = new DateProcessor("eventdate","syear","smonth","sday");
	//private AbstractDataProcessor altitudeProcessor = new NumericPairDataProcessor("minimumelevationinmeters","maximumelevationinmeters");
	//private DegreeMinuteToDecimalProcessor dmsProcessor = new DegreeMinuteToDecimalProcessor();
	
	private static Map<Country,StateProvinceProcessor<?>> stateProvinceProcessorMap = new HashMap<Country,StateProvinceProcessor<?>>();
	static{
		stateProvinceProcessorMap.put(Country.CANADA, new StateProvinceProcessor<CAProvince>(Country.CANADA, CAProvince.class));
		stateProvinceProcessorMap.put(Country.BELGIUM, new StateProvinceProcessor<BEProvince>(Country.BELGIUM, BEProvince.class));
	}
	
	@Override
	public OccurrenceQualityReportElement process(OccurrenceRawModel rawModel, Map<SharedParameterEnum,Object> sharedParameters) {
		OccurrenceQualityReportElement occurrenceQualityReportElement = new OccurrenceQualityReportElement();
		ProcessingResult pr = new ProcessingResult();
		Country country = null;
		if(StringUtils.isNotBlank(rawModel.getCountry())){
			//Country processing
			country = countryProcessor.process(rawModel.getCountry(),pr);
			if(country != null){
				handleProcessResult(COUNTRY,country.getTitle(),rawModel.getCountry(),occurrenceQualityReportElement);
			}
			else{
				handleProcessResult(COUNTRY,null,rawModel.getCountry(),occurrenceQualityReportElement);
			}
		}
		else{
			occurrenceQualityReportElement.addOccurrenceQualityReportElementResult(COUNTRY, OccurrenceQualityReportElement.QualityStatusEnum.MISSING);
		}
		
		//Stateprovince processing
		if(StringUtils.isNotBlank(rawModel.getStateprovince())){
			if(country != null){
				if(stateProvinceProcessorMap.get(country) != null){
					StateProvinceEnum stateProvince = stateProvinceProcessorMap.get(country).process(rawModel.getStateprovince(), pr);
					if(stateProvince != null){
						handleProcessResult(STATE_PROVINCE,stateProvince.getName(),rawModel.getStateprovince(),occurrenceQualityReportElement);
					}
					else{
						handleProcessResult(STATE_PROVINCE,null,rawModel.getStateprovince(),occurrenceQualityReportElement);
					}
				}
				else{
					occurrenceQualityReportElement.addOccurrenceQualityReportElementResult(STATE_PROVINCE, OccurrenceQualityReportElement.QualityStatusEnum.NOT_IMPLEMENTED);
				}
			}
			else{
				occurrenceQualityReportElement.addOccurrenceQualityReportElementResult(STATE_PROVINCE, OccurrenceQualityReportElement.QualityStatusEnum.DEPENDENCY_MISSING);
			}
		}
		else{
			occurrenceQualityReportElement.addOccurrenceQualityReportElementResult(STATE_PROVINCE, OccurrenceQualityReportElement.QualityStatusEnum.MISSING);
		}
		return occurrenceQualityReportElement;
	}
	@Override
	public void init() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void destroy() {
		// TODO Auto-generated method stub
		
	}
	
	private void handleProcessResult(String term, String result, String raw, OccurrenceQualityReportElement occurrenceQualityReportElement){
		if(result == null){
			occurrenceQualityReportElement.addOccurrenceQualityReportElementResult(term, OccurrenceQualityReportElement.QualityStatusEnum.UNPROCESSABLE);
		}
		else{
			if(result.equalsIgnoreCase(raw)){
				occurrenceQualityReportElement.addOccurrenceQualityReportElementResult(term, OccurrenceQualityReportElement.QualityStatusEnum.OK);
			}
			else{
				occurrenceQualityReportElement.addOccurrenceQualityReportElementResult(term, OccurrenceQualityReportElement.QualityStatusEnum.PROCESSABLE);
			}
		}
	}

}
