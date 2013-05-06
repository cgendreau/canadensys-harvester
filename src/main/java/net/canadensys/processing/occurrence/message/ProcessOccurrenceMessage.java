package net.canadensys.processing.occurrence.message;

import java.util.ArrayList;
import java.util.List;

import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;
import net.canadensys.processing.message.ProcessingMessageIF;

/**
 * Message asking to process raw occurrence data.
 * @author canadensys
 *
 */
public class ProcessOccurrenceMessage implements ProcessingMessageIF{
	private String when;
	
	private List<OccurrenceRawModel> rawModelList;
	
	public ProcessOccurrenceMessage(){
		rawModelList = new ArrayList<OccurrenceRawModel>();
	}
	
	public String getWhen() {
		return when;
	}
	public void setWhen(String when) {
		this.when = when;
	}
	
	public List<OccurrenceRawModel> getRawModelList() {
		return rawModelList;
	}
	public void getRawModelList(List<OccurrenceRawModel> rawModelList) {
		this.rawModelList = rawModelList;
	}
	public void addRawModel(OccurrenceRawModel rawModel) {
		rawModelList.add(rawModel);
	}
}
