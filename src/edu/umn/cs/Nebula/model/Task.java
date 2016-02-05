package edu.umn.cs.Nebula.model;

import java.util.Date;

public class Task {
	private int id;
	private int jobId;
	private boolean active;
	private boolean complete;
	private Date postedTime;
	private Date startTime;
	private Date completedTime;
	private String completedBy;
	private String inputFile;
	private String executableFile;
	private String status;
	
	public Task(int id, int jobId) {
		this.id = id;
		this.jobId = jobId;
		active = true;
		complete = false;
		postedTime = new Date();
		completedBy = null;
	}
	
	public Task(int id, int jobId, String inputFile, String executableFile) {
		this.id = id;
		this.jobId = jobId;
		this.inputFile = inputFile;
		this.executableFile = executableFile;
		active = true;
		complete = false;
		postedTime = new Date();
		completedBy = null;
	}
	
	public int getId() {
		return id;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public boolean isActive() {
		return active;
	}

	public void setComplete(boolean complete) {
		completedTime = new Date();
		this.complete = complete;
	}

	public boolean isComplete() {
		return complete;
	}
	
	public void setStartTime() {
		this.startTime = new Date();
	}
	
	public Date getStartTime() {
		return startTime;
	}
	
	public Date getPostedTime() {
		return postedTime;
	}
	
	public Date getCompletedTime() {
		return completedTime;
	}

	public void setCompletedBy(String nodeId) {
		completedBy = nodeId;
	}
	
	public String getCompletedBy() {
		return completedBy;
	}

	public String getInputFile() {
		return inputFile;
	}

	public void setInputFile(String inputFile) {
		this.inputFile = inputFile;
	}

	public String getExecutableFile() {
		return executableFile;
	}

	public void setExecutableFile(String executableFile) {
		this.executableFile = executableFile;
	}

	public int getJobId() {
		return jobId;
	}

	public void setJobId(int jobId) {
		this.jobId = jobId;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}
}
