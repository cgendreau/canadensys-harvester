package net.canadensys.processing.occurrence.processor;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import net.canadensys.dataportal.occurrence.model.OccurrenceModel;
import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;
import net.canadensys.processing.ItemProcessorIF;
import net.canadensys.processing.occurrence.SharedParameterEnum;
import net.canadensys.processor.AbstractDataProcessor;
import net.canadensys.processor.ProcessingResult;
import net.canadensys.processor.datetime.DateProcessor;
import net.canadensys.processor.geography.CountryContinentProcessor;
import net.canadensys.processor.geography.CountryProcessor;
import net.canadensys.processor.geography.DecimalLatLongProcessor;
import net.canadensys.processor.geography.DegreeMinuteToDecimalProcessor;
import net.canadensys.processor.geography.StateProvinceProcessor;
import net.canadensys.processor.numeric.NumericPairDataProcessor;
import net.canadensys.vocabulary.Continent;
import net.canadensys.vocabulary.stateprovince.BEProvince;
import net.canadensys.vocabulary.stateprovince.CAProvince;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math.util.MathUtils;
import org.apache.log4j.Logger;
import org.gbif.api.model.vocabulary.Country;
import org.gbif.ecat.model.ParsedName;
import org.gbif.ecat.parser.NameParser;
import org.gbif.ecat.parser.UnparsableException;
import org.gbif.ecat.voc.NameType;

/**
 * Processing each OccurrenceRawModel into OccurrenceModel.
 * @author canadensys
 *
 */
public class OccurrenceProcessor implements ItemProcessorIF<OccurrenceRawModel, OccurrenceModel>{

	//get log4j handler
	private static final Logger LOGGER = Logger.getLogger(OccurrenceProcessor.class);
	
	private static final Integer MIN_DATE = 1700;
	private static final Integer MAX_DATE = Calendar.getInstance().get(Calendar.YEAR);
	
	private NameParser GBIF_NAME_PARSER = new NameParser();
	
	//Processors
	private CountryProcessor countryProcessor = new CountryProcessor();
	private CountryContinentProcessor countryContinentProcessor = new CountryContinentProcessor();
	private AbstractDataProcessor latLongProcessor = new DecimalLatLongProcessor("decimallatitude","decimallongitude");
	private DateProcessor dateProcessor = new DateProcessor("eventdate","syear","smonth","sday");
	private AbstractDataProcessor altitudeProcessor = new NumericPairDataProcessor("minimumelevationinmeters","maximumelevationinmeters");
	private DegreeMinuteToDecimalProcessor dmsProcessor = new DegreeMinuteToDecimalProcessor();
	
	private static Map<String,AbstractDataProcessor> stateProvinceProcessorMap = new HashMap<String,AbstractDataProcessor>();
	static{
		stateProvinceProcessorMap.put(Country.CANADA.getTitle(), new StateProvinceProcessor<CAProvince>(Country.CANADA, CAProvince.class));
		stateProvinceProcessorMap.put(Country.BELGIUM.getTitle(), new StateProvinceProcessor<BEProvince>(Country.BELGIUM, BEProvince.class));
	}
	
	@Override
	public void init() {};
		
	@Override
	public OccurrenceModel process(OccurrenceRawModel rawModel, Map<SharedParameterEnum,Object> sharedParameters) {
		
		//cleaning report?
		OccurrenceModel cleanedModel = new OccurrenceModel();
		
		//keep the same auto_id 
		cleanedModel.setAuto_id(rawModel.getAuto_id());
		
		//set a cleaned associatedmedia
		cleanedModel.setAssociatedmedia(normalizeURLSeparator(rawModel.getAssociatedmedia()));
		
		cleanedModel.setCatalognumber(rawModel.getCatalognumber());
		cleanedModel.setCollectioncode(rawModel.getCollectioncode());
		cleanedModel.set_references(normalizeURLSeparator(rawModel.get_references()));
		
		//Country processing
		countryProcessor.processBean(rawModel, cleanedModel, null, null);

		//Continent processing
		if(!StringUtils.isBlank(rawModel.getContinent())){
			cleanedModel.setContinent(rawModel.getContinent());
		}
		else{
			Continent continent = null;
			try{
				if(!StringUtils.isBlank(cleanedModel.getCountry())){
					Country country = Country.valueOf(cleanedModel.getCountry().toUpperCase().replaceAll(" ", "_"));
					continent = countryContinentProcessor.process(country.getIso2LetterCode(), null);
					if(continent != null){
						cleanedModel.setContinent(continent.getTitle());
					}
				}
			}
			catch(IllegalArgumentException ignore){}
		}
		
		//state or province processing
		if(!StringUtils.isBlank(cleanedModel.getCountry()) && stateProvinceProcessorMap.get(cleanedModel.getCountry()) != null){
			stateProvinceProcessorMap.get(cleanedModel.getCountry()).processBean(rawModel, cleanedModel, null, null);
		}
		else{//if we can't process it, copy it 
			cleanedModel.setStateprovince(rawModel.getStateprovince());
		}
		
		cleanedModel.setCounty(rawModel.getCounty());
		cleanedModel.setMunicipality(rawModel.getMunicipality());
		cleanedModel.setDatasetname(rawModel.getDatasetname());
		
		cleanedModel.setKingdom(rawModel.getKingdom());
		cleanedModel.setPhylum(rawModel.getPhylum());
		cleanedModel.set_class(rawModel.get_class());
		cleanedModel.set_order(rawModel.get_order());
		cleanedModel.setFamily(rawModel.getFamily());
		cleanedModel.setGenus(rawModel.getGenus());
		cleanedModel.setSpecificepithet(rawModel.getSpecificepithet());
		cleanedModel.setInfraspecificepithet(rawModel.getInfraspecificepithet());
		
		cleanedModel.setLocality(rawModel.getLocality());
		
		cleanedModel.setRecordedby(rawModel.getRecordedby());
		cleanedModel.setRecordnumber(rawModel.getRecordnumber());
		cleanedModel.setInstitutioncode(rawModel.getInstitutioncode());
		cleanedModel.setTaxonrank(rawModel.getTaxonrank());
		cleanedModel.setEventdate(rawModel.getEventdate());
		cleanedModel.setHasmedia(!StringUtils.isBlank(rawModel.getAssociatedmedia()));
		
		cleanScientificName(rawModel, cleanedModel);
		
		//Process date
		processDate(rawModel, cleanedModel);
		
		processCoordinates(rawModel, cleanedModel);
		
		processAltitude(rawModel,cleanedModel);
		
		cleanedModel.setVerbatimelevation(rawModel.getVerbatimelevation());
		cleanedModel.setHabitat(rawModel.getHabitat());
		
		cleanedModel.setDwcaid(rawModel.getDwcaid());
		cleanedModel.setSourcefileid(rawModel.getSourcefileid());
		return cleanedModel;
	}
	
	/**
	 * This method will normalize the separator(in case of multiple URLs) of an URL field.
	 * TODO : test the URL?
	 * @param rawURLField
	 * @return
	 */
	private String normalizeURLSeparator(String rawURLField){
		if(StringUtils.isBlank(rawURLField)){
			return rawURLField;
		}
		String[] urls = rawURLField.split("\\|");
		ArrayList<String> urlList = new ArrayList<String>();
		for(String url : urls){
			if(!StringUtils.isBlank(url)){
				urlList.add(url.trim());
			}
		}
		return StringUtils.join(urlList,"; ");
	}
	
	/**
	 * This method will try to clean the scientific name.
	 * If possible, this method will split the raw scientificname into 2 parts : -scientificname and scientificnameauthorship.
	 * The raw scientificname will be set into the rawscientificname.
	 * If it's not possible to parse this scientific name, the raw scientific name will be kept in scientificname
	 * and will also be in rawscientificname.
	 * @param rawModel
	 * @param occModel
	 */
	private void cleanScientificName(OccurrenceRawModel rawModel, OccurrenceModel occModel){
		occModel.setRawscientificname(rawModel.getScientificname());
		//set it to raw scientificname in case the parsing could not be done
		occModel.setScientificname(rawModel.getScientificname());
		
		ParsedName<String> parsedName = null;
		try{
			parsedName = GBIF_NAME_PARSER.parse(rawModel.getScientificname());
			if (NameType.wellformed.equals(parsedName.getType())
					|| NameType.sciname.equals(parsedName.getType())) {
				occModel.setScientificname(parsedName.canonicalNameWithMarker());
				occModel.setScientificnameauthorship(parsedName.authorshipComplete());
				
				//Set the species from the parser
				occModel.setSpecies(StringUtils.trim(parsedName.getGenusOrAbove() + " " + parsedName.getSpecificEpithet()));
			}
		}
		catch(UnparsableException uEx){
			System.out.println("NameParser " + uEx.getMessage());
		}
	}
	
	private void processDate(OccurrenceRawModel rawModel, OccurrenceModel occModel){
		ProcessingResult result = new ProcessingResult();
		
		dateProcessor.processBean(rawModel, occModel, null, result);
		
		if(occModel.getSday() == null && occModel.getSmonth() == null&& occModel.getSyear() == null){
			if(result.getErrorList().size() > 0){
				System.out.println(result.getErrorString());
			}
			else{//try verbatim date
				Integer[] pDate = dateProcessor.process(rawModel.getVerbatimeventdate(), result);
				occModel.setSyear(pDate[DateProcessor.YEAR_IDX]);
				occModel.setSmonth(pDate[DateProcessor.MONTH_IDX]);
				occModel.setSday(pDate[DateProcessor.DAY_IDX]);
			}
		}
		
		secondPassDateProcess(occModel);
	}
	
	/**
	 * //lat long parsing
	 * @param rawModel
	 * @param occModel
	 */
	private void processCoordinates(OccurrenceRawModel rawModel, OccurrenceModel occModel){
		//will be set to true later if we find valid coordinates
		occModel.setHascoordinates(false);
		
		ProcessingResult result = new ProcessingResult();
		latLongProcessor.processBean(rawModel, occModel, null, result);
		//only check latitude since if the coordinate is not valid, both will be null
		if(occModel.getDecimallatitude() != null){
			occModel.setHascoordinates(true);
		}
		else{
			if(result.getErrorList().size() > 0){
				System.out.println(result.getErrorString());
			}
			else{//try verbatim
				Double[] latLong = dmsProcessor.process(rawModel.getVerbatimlatitude(),rawModel.getVerbatimlongitude(), result);
				
				if(latLong[0] != null && latLong[1] != null){
					if(result.getErrorList().size() > 0){
						System.out.println(result.getErrorString());
					}
					else{
						System.out.println("Good one -> " + rawModel.getVerbatimlatitude() +"," + rawModel.getVerbatimlongitude());
						occModel.setDecimallatitude(latLong[0]);
						occModel.setDecimallongitude(latLong[1]);
						occModel.setHascoordinates(true);
					}
				}
			}
		}
	}
	
	private void processAltitude(OccurrenceRawModel rawModel, OccurrenceModel occModel){
		
		altitudeProcessor.processBean(rawModel, occModel, null, null);
		
		Double avgElevationDouble = null;
		Double minElevationDouble = occModel.getMinimumelevationinmeters();
		Double maxElevationDouble = occModel.getMaximumelevationinmeters();
		
		if(minElevationDouble != null){
			if(maxElevationDouble == null){
				occModel.setMaximumelevationinmeters(minElevationDouble);
			}
		}
		if(maxElevationDouble != null){
			if(minElevationDouble == null){
				occModel.setMinimumelevationinmeters(maxElevationDouble);
			}
		}
		//compute the rounded average elevation 
		if(minElevationDouble !=null && maxElevationDouble!=null){
			avgElevationDouble = (minElevationDouble+maxElevationDouble)/2d;
			occModel.setAveragealtituderounded(((int)MathUtils.round(avgElevationDouble/100d,0))*100);
		}
	}
	
	/**
	 * 
	 * @param occModel
	 */
	private void secondPassDateProcess(OccurrenceModel occModel){		
		//make sure the year is valid
		if(occModel.getSyear() !=null && occModel.getSyear() < MIN_DATE){
			occModel.setSyear(null);
		}
		if(occModel.getSyear() !=null && occModel.getSyear() > MAX_DATE){
			occModel.setSyear(null);
		}
		
		//set the decade
		if(occModel.getSyear() != null){
			occModel.setDecade((occModel.getSyear()/10)*10);
		}
	}
	
	@Override
	public void destroy() {};

}
