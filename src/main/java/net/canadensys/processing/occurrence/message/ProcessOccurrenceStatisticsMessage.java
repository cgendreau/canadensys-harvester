package net.canadensys.processing.occurrence.message;

import java.util.ArrayList;
import java.util.List;

import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;
import net.canadensys.processing.message.ProcessingMessageIF;

/**
 * Message asking to process statistics on raw occurrence data.
 * @author canadensys
 *
 */
public class ProcessOccurrenceStatisticsMessage implements ProcessingMessageIF{
	private List<OccurrenceRawModel> rawModelList;
	
	public ProcessOccurrenceStatisticsMessage(){
		rawModelList = new ArrayList<OccurrenceRawModel>();
	}
	
	public List<OccurrenceRawModel> getRawModelList() {
		return rawModelList;
	}
	public void setRawModelList(List<OccurrenceRawModel> rawModelList) {
		this.rawModelList = rawModelList;
	}
	public void addRawModel(OccurrenceRawModel rawModel) {
		rawModelList.add(rawModel);
	}
}
