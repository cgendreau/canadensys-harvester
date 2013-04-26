package net.canadensys.processing;

import java.util.Map;

/**
 * Item reading interface
 * @author canadensys
 *
 * @param <T> type of object to read
 */
public interface ItemReaderIF<T> {
	
	public void open(Map<String,Object> sharedParameters);
	public void close();
	
	public T read();
}
