package edu.umn.cs.Nebula.job;

public class RunningTask extends Task {
	private String nodeId;
	
	public RunningTask(long taskId, JobType type, TaskStatus status) {
		super(taskId, type, status);
	}
	
	public RunningTask(long taskId, JobType type, String command, String exec, TaskStatus status) {
		super(taskId, type, command, exec, status);
	}
	
	public RunningTask(int taskId, int jobId, JobType type, String command, String executableFile, TaskStatus status) {
		super(taskId, jobId, type, command, executableFile, status);
	}

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}
}
