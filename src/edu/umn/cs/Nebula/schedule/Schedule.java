package edu.umn.cs.Nebula.schedule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import edu.umn.cs.Nebula.job.Task;

public class Schedule {
	private HashMap<String, ArrayList<Task>> map;
	
	public Schedule() {
		map = new HashMap<String, ArrayList<Task>>();
	}
	
	public ArrayList<Task> getList(String nodeId) {
		for (String key: map.keySet()) {
			if (key.equals(nodeId)) {
				return map.get(key);
			}
		}
		return null;
	}
	
	public Set<String> getNodes() {
		return map.keySet();
	}
	
	public int addTask(String nodeId, Task task) {
		for (String node: map.keySet()) {
			if (node.equals(nodeId)) {
				map.get(node).add(task);
				return map.get(node).size();
			}
		}
		map.put(nodeId, new ArrayList<Task>());
		map.get(nodeId).add(task);

		return map.get(nodeId).size();
	}
	
	public boolean removeTask(String nodeId, int taskId) {		
		for (String key: map.keySet()) {
			if (key.equals(nodeId) && map.get(key) != null && !map.get(key).isEmpty()) {
				for (Task task: map.get(key)) {
					if (task.getId() == taskId) {
						return map.get(key).remove(task);
					}
				}
			}
		}
		return false;
	}
	
	public Task getFirstTask(String nodeId) {
		for (String key: map.keySet()) {
			if (key.equals(nodeId) && map.get(key) != null && !map.get(key).isEmpty()) {
				return map.get(key).get(0);
			}
		}
		return null;
	}
	
	public boolean isEmpty(String nodeId) {
		if (!map.containsKey(nodeId) || map.get(nodeId) == null || map.get(nodeId).isEmpty()) {
			return true;
		}
		return false;
	}
}
