package net.canadensys.processing;

import java.util.Map;

import net.canadensys.processing.occurrence.SharedParameterEnum;


/**
 * Item reading interface
 * @author canadensys
 *
 * @param <T> type of object to read
 */
public interface ItemReaderIF<T> {
	
	public void open(Map<SharedParameterEnum,Object> sharedParameters);
	public void close();
	
	public T read();
}
