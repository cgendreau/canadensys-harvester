package net.canadensys.processing;

import java.util.List;

/**
 * Item writing interface
 * @author canadensys
 *
 * @param <T> type of object to write
 */
public interface ItemWriterIF<T> {
	public void open();
	public void close();
	
	public void write(List<? extends T> elementList);
	public void write(T element);
}

