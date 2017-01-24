package edu.umn.cs.Nebula.job;

import java.util.Date;
import java.util.HashMap;

public class Application {
	private long id;
	private String name;
	private ApplicationType applicationType;
	private boolean isActive;
	private boolean isCompleted;
	private Date postedTime;
	private Date startTime;
	private Date completedTime;
	private HashMap<Long, Job> jobs;

	public Application(long id, String name, ApplicationType type) {
		this.id = id;
		this.name = name;
		this.applicationType = type;
		this.isActive = true;
		this.isCompleted = false;
		this.jobs = new HashMap<Long, Job>();
		this.postedTime = new Date();
	}
	
	public long getId() {
		return id;
	}
	
	public void setId(long id) {
		this.id = id;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public ApplicationType getApplicationType() {
		return applicationType;
	}

	public void setApplicationType(ApplicationType applicationType) {
		this.applicationType = applicationType;
	}
	
	public void setActive(boolean active) {
		this.isActive = active;
	}

	public boolean isActive() {
		return isActive;
	}

	public void setComplete(boolean complete) {
		this.isCompleted = complete;
		completedTime = new Date();
	}

	public boolean isComplete() {
		return isCompleted;
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

	public HashMap<Long, Job> getJobs() {
		return jobs;
	}
	
	public void setJobs(HashMap<Long, Job> jobs) {
		this.jobs = jobs;
	}

	public int addJob(Job job) {
		jobs.put(job.getId(), job);
		return jobs.size();
	}

	public int removeJob(int jobId) {
		jobs.remove(jobId);
		return jobs.size();
	}

	public Job getJob(int jobId) {
		return jobs.get(jobId);
	}

	public int getNumJobs() {
		return jobs.size();
	}
}
