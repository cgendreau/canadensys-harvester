package net.canadensys.processing.occurrence.job;

import java.util.HashMap;
import java.util.Map;

import net.canadensys.processing.ItemTaskIF;
import net.canadensys.processing.occurrence.SharedParameterEnum;

import org.springframework.beans.factory.annotation.Autowired;

public class FindUsedDwcaTermJob {
	
	protected Map<SharedParameterEnum,Object> sharedParameters = new HashMap<SharedParameterEnum, Object>();
	
	//Task and step
	@Autowired
	private ItemTaskIF findUsedDwcaTermTask;
	
	public void doJob(){
		findUsedDwcaTermTask.execute(sharedParameters);
	}

}
