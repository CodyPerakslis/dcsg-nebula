package edu.umn.cs.Nebula.cache;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class LRUCache {
	
	private final int maxSize;
	private LinkedList<String> index;
	private HashMap<String, Object> data;
	
	public LRUCache(int size) {
		maxSize = size;
		index = new LinkedList<String>();
		data = new HashMap<String, Object>();
	}
	
	public void add(String key, Object value) {
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
	
	public Object remove(String key) {
		index.remove(key);
		return data.remove(key);
	}
	
	public Object removeOldest() {
		String removedKey = index.removeFirst();
		return data.remove(removedKey);
	}
	
	public boolean containsKey(String key) {
		return data.containsKey(key);
	}
	
	public List<String> getKeys() {
		return index;
	}
}
