package net.canadensys.processing.occurrence.model;

public class ApplicationStatus {
	
	public static enum JobStatusEnum {UNDEFINED,WAITING,RUNNING,DONE_ERROR,DONE_SUCCESS};
	
	private JobStatusEnum importStatus = JobStatusEnum.UNDEFINED;
	private JobStatusEnum moveStatus = JobStatusEnum.UNDEFINED;
	
	
	public JobStatusEnum getImportStatus() {
		return importStatus;
	}
	public void setImportStatus(JobStatusEnum importStatus) {
		this.importStatus = importStatus;
	}
	
	public JobStatusEnum getMoveStatus() {
		return moveStatus;
	}
	public void setMoveStatus(JobStatusEnum moveStatus) {
		this.moveStatus = moveStatus;
	}
	
	public void resetStatus(){
		importStatus = JobStatusEnum.UNDEFINED;
		moveStatus = JobStatusEnum.UNDEFINED;
	}

}
