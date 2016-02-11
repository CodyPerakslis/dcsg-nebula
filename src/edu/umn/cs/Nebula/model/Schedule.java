package edu.umn.cs.Nebula.model;

import java.util.ArrayList;
import java.util.HashMap;

import edu.umn.cs.Nebula.application.Task;

public class Schedule {
	private HashMap<String, ArrayList<Task>> map;
	
	public Schedule() {
		map = new HashMap<String, ArrayList<Task>>();
	}
	
	public ArrayList<Task> getList(String nodeId) {
		return map.get(nodeId);
	}
	
	public int addTask(String nodeId, Task task) {
		if (!map.containsKey(nodeId) || map.get(nodeId) == null) {
			map.put(nodeId, new ArrayList<Task>());
		}
		map.get(nodeId).add(task);
		
		return map.get(nodeId).size();
	}
	
	public boolean removeTask(String nodeId, Task task) {
		if (!map.containsKey(nodeId) || map.get(nodeId) == null || map.get(nodeId).isEmpty()) {
			return false;
		}
		
		return map.get(nodeId).remove(task);
	}
	
	public boolean removeTask(String nodeId, int taskId) {
		if (!map.containsKey(nodeId) || map.get(nodeId) == null || map.get(nodeId).isEmpty()) {
			return false;
		}
		
		for (Task task: map.get(nodeId)) {
			if (task.getId() == taskId) {
				map.get(nodeId).remove(task);
				return true;
			}
		}
		return false;
	}
	
	public Task getFirstTask(String nodeId) {
		if (!map.containsKey(nodeId) || map.get(nodeId) == null || map.get(nodeId).isEmpty()) {
			return null;
		}
		
		return map.get(nodeId).get(0);
	}
	
	public boolean isEmpty(String nodeId) {
		if (!map.containsKey(nodeId) || map.get(nodeId) == null || map.get(nodeId).isEmpty()) {
			return true;
		}
		return false;
	}
}
