package net.canadensys.processing.occurrence.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import net.canadensys.processing.occurrence.model.OccurrenceQualityReportElement.QualityStatusEnum;

public class OccurrenceQualityReport {
	
	//map fields -> map QualityStatusEnum -> qty
	
	private Map<String,Map<OccurrenceQualityReportElement.QualityStatusEnum,Integer>> fieldsElementMap;
	
	public OccurrenceQualityReport() {
		fieldsElementMap = new HashMap<String, Map<QualityStatusEnum,Integer>>();
	}
	
	public void addReportElement(OccurrenceQualityReportElement element){
		//all fields name
		Set<String> fieldKeys = element.getOccurrenceQualityReportElements().keySet();
		
		Map<OccurrenceQualityReportElement.QualityStatusEnum,Integer> currFieldQualityReportElementMap;
		OccurrenceQualityReportElement.QualityStatusEnum currFieldQualityReportElement;
		for(String currField : fieldKeys){
			currFieldQualityReportElementMap = fieldsElementMap.get(currField);
			if(currFieldQualityReportElementMap == null){
				currFieldQualityReportElementMap = new HashMap<OccurrenceQualityReportElement.QualityStatusEnum, Integer>();
				fieldsElementMap.put(currField, currFieldQualityReportElementMap);
			}
			currFieldQualityReportElement = element.getOccurrenceQualityReportElements().get(currField);
			if(currFieldQualityReportElementMap.get(currFieldQualityReportElement) == null){
				currFieldQualityReportElementMap.put(currFieldQualityReportElement, 1);
			}
			else{
				currFieldQualityReportElementMap.put(currFieldQualityReportElement, currFieldQualityReportElementMap.get(currFieldQualityReportElement)+1);
			}
		}
	}
	
	public void printReport(){
		System.out.println("##QUALITY REPORT##");
		for(String currField : fieldsElementMap.keySet()){
			System.out.println("Field : "+currField+"---------------");
			StringBuilder sb = new StringBuilder();
			for(OccurrenceQualityReportElement.QualityStatusEnum currQualityStatusEnum : fieldsElementMap.get(currField).keySet()){
				sb.append(currQualityStatusEnum+":"+fieldsElementMap.get(currField).get(currQualityStatusEnum)+",");
			}
			sb.deleteCharAt(sb.length()-1);
			System.out.println("["+sb.toString()+"]");
		}
	}

}
