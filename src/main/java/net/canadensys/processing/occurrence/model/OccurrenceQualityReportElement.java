package net.canadensys.processing.occurrence.model;

import java.util.HashMap;
import java.util.Map;

/**
 * Model containing the quality report for one occurrence.
 * @author canadensys
 *
 */
public class OccurrenceQualityReportElement {
	public enum QualityStatusEnum {MISSING,OK,PARSABLE,UNPARSABLE}
	
	private Map<String,QualityStatusEnum> qualityReportElementFields;
	
	public OccurrenceQualityReportElement(){
		qualityReportElementFields = new HashMap<String, OccurrenceQualityReportElement.QualityStatusEnum>();
	}

	public void addOccurrenceQualityReportElementResult(String field, QualityStatusEnum qualityStatus){
		qualityReportElementFields.put(field,qualityStatus);
	}
}
