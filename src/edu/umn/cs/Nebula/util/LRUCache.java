package edu.umn.cs.Nebula.util;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class LRUCache<E> {
	
	private final int maxSize;
	private LinkedList<E> index;
	private HashMap<E, Object> data;
	
	public LRUCache(int size) {
		maxSize = size;
		index = new LinkedList<E>();
		data = new HashMap<E, Object>();
	}
	
	public void add(E key, Object value) {
		if (!data.containsKey(key)) {
			if (index.size() == maxSize) {
				removeOldest();
			}
		} else {
			index.remove(key);
		}
		index.addLast(key);
		data.put(key, value);
	}
	
	public Object remove(E key) {
		index.remove(key);
		return data.remove(key);
	}
	
	public Object get(E key) {
		index.remove(key);
		index.addLast(key);
		return data.get(key);
	}
	
	public Object removeOldest() {
		E removedKey = index.removeFirst();
		return data.remove(removedKey);
	}
	
	public boolean containsKey(E key) {
		return data.containsKey(key);
	}
	
	public List<E> getKeys() {
		return index;
	}
}
