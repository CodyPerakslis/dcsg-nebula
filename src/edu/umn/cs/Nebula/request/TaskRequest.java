package edu.umn.cs.Nebula.request;

import java.util.LinkedHashMap;

import edu.umn.cs.Nebula.job.RunningTask;
import edu.umn.cs.Nebula.job.TaskInfo;

public class TaskRequest {
	private RunningTask task;
	private TaskRequestType type;
	private LinkedHashMap<String, TaskInfo> taskStatuses;
	
	public TaskRequest(RunningTask task, TaskRequestType type) {
		setTask(task);
		setType(type);
		this.setTaskStatuses(new LinkedHashMap<String, TaskInfo>());
	}
	
	public TaskRequest(TaskRequestType type) {
		setTask(null);
		setType(type);
		this.setTaskStatuses(new LinkedHashMap<String, TaskInfo>());
	}
	
	public RunningTask getTask() {
		return task;
	}
	
	public void setTask(RunningTask task) {
		this.task = task;
	}

	public TaskRequestType getType() {
		return type;
	}

	public void setType(TaskRequestType type) {
		this.type = type;
	}

	public LinkedHashMap<String, TaskInfo> getTaskStatuses() {
		return taskStatuses;
	}

	public void setTaskStatuses(LinkedHashMap<String, TaskInfo> taskStatuses) {
		this.taskStatuses = taskStatuses;
	}
	
	public void addTaskInfo(String processId, TaskInfo info) {
		taskStatuses.put(processId, info);
	}

}
