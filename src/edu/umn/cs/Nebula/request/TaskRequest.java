package edu.umn.cs.Nebula.request;

public class TaskRequest {
	private String nodeId;
	private int taskId;
	private TaskRequestType requestType;
	private String status;
	
	public TaskRequest(TaskRequestType type, String nodeId, int taskId) {
		this.requestType = type;
		this.nodeId = nodeId;
		this.taskId = taskId;
	}

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public int getTaskId() {
		return taskId;
	}

	public void setTaskId(int taskId) {
		this.taskId = taskId;
	}

	public TaskRequestType getType() {
		return requestType;
	}

	public void setType(TaskRequestType requestType) {
		this.requestType = requestType;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}
}
