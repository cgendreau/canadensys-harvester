package net.canadensys.processing.occurrence.model;

import java.util.HashMap;
import java.util.Map;

/**
 * Model containing the quality report for one occurrence.
 * @author canadensys
 *
 */
public class OccurrenceQualityReportElement {
	public enum QualityStatusEnum {
		MISSING, // the value is missing
		OK, //provided value is ok
		PROCESSABLE, //provided value can be processed into usable value
		UNPROCESSABLE, // the provided value can not be processed
		DEPENDENCY_MISSING,
		NOT_IMPLEMENTED
	}
	
	private Map<String,QualityStatusEnum> qualityReportElementFields;
	
	public OccurrenceQualityReportElement(){
		qualityReportElementFields = new HashMap<String, OccurrenceQualityReportElement.QualityStatusEnum>();
	}

	public void addOccurrenceQualityReportElementResult(String field, QualityStatusEnum qualityStatus){
		qualityReportElementFields.put(field,qualityStatus);
	}
	
	public Map<String,QualityStatusEnum> getOccurrenceQualityReportElements(){
		return qualityReportElementFields;
	}
}
