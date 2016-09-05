package edu.umn.cs.Nebula.mobile;

import java.util.LinkedList;

public class GridCell {

	private final String id;
	private LinkedList<String> items;
	
	public GridCell(String id) {
		this.id = id;
		items = new LinkedList<String>();
	}
	
	public boolean addItem(String item) {
		if (items.contains(item))
			return true;
		return items.add(item);
	}
	
	public boolean removeItem(String item) {
		return items.remove(item);
	}
	
	public String getItem(int i) {
		return items.get(i);
	}
	
	public LinkedList<String> getAllItems() {
		return items;
	}
	
	public int getNumItems() {
		return items.size();
	}
	
	public void clearItems() {
		items.clear();
	}

	public String getId() {
		return id;
	}
}
