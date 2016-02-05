package edu.umn.cs.Nebula.request;

import java.util.HashMap;

import edu.umn.cs.Nebula.model.Task;

public class ScheduleRequest {
	private HashMap<String, Task> nodeTask;
	
	public ScheduleRequest() {
		nodeTask = new HashMap<String, Task>();
	}

	public HashMap<String, Task> getNodeTask() {
		return nodeTask;
	}

	public void setNodeTask(HashMap<String, Task> nodeTask) {
		this.nodeTask = nodeTask;
	}
}
