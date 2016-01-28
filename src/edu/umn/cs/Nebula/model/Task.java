package edu.umn.cs.Nebula.model;

import java.util.Date;

public class Task {
	private int id;
	private boolean active;
	private boolean complete;
	private Date postedTime;
	private Date startTime;
	private Date completedTime;
	private Integer completedBy;
	private String inputFile;
	
	public Task(int id, String inputFile) {
		this.id = id;
		this.setInputFile(inputFile);
		active = true;
		complete = false;
		postedTime = new Date();
		completedBy = -1;
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

	public void setCompletedBy(int nodeId) {
		completedBy = nodeId;
	}
	
	public int getCompletedBy() {
		return completedBy;
	}

	public String getInputFile() {
		return inputFile;
	}

	public void setInputFile(String inputFile) {
		this.inputFile = inputFile;
	}
}
