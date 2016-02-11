package edu.umn.cs.Nebula.application;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

public class Application {
	public final static int DEFAULT_PRIORITY = 5;
	
	private int id;
	private String name;
	private ApplicationType applicationType;
	private int priority;
	private boolean active;
	private boolean complete;
	private Date postedTime;
	private Date startTime;
	private Date completedTime;
	private HashMap<Integer, Job> jobs;
	private ArrayList<String> fileList;

	public Application(int id, String name, ApplicationType type) {
		this.id = id;
		this.name = name;
		this.applicationType = type;
		this.priority = DEFAULT_PRIORITY;
		active = true;
		complete = false;
		jobs = new HashMap<Integer, Job>();
		postedTime = new Date();
		fileList = new ArrayList<String>();
	}
	
	public Application(int id, String name, ApplicationType type, int priority) {
		this.id = id;
		this.name = name;
		this.applicationType = type;
		this.priority = priority;
		active = true;
		complete = false;
		jobs = new HashMap<Integer, Job>();
		postedTime = new Date();
		fileList = new ArrayList<String>();
	}

	public Application(int id, String name, ApplicationType type, int priority, HashMap<Integer, Job> jobs) {
		this.id = id;
		this.name = name;
		this.applicationType = type;
		this.priority = priority;
		active = true;
		complete = false;
		this.jobs = jobs;
		postedTime = new Date();
		fileList = new ArrayList<String>();
	}
	
	public int getId() {
		return id;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}

	public int getPriority() {
		return priority;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public boolean isActive() {
		return active;
	}

	public void setComplete(boolean complete) {
		this.complete = complete;
		completedTime = new Date();
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

	public HashMap<Integer, Job> getJobs() {
		return jobs;
	}
	
	public void setJobs(HashMap<Integer, Job> jobs) {
		this.jobs = jobs;
	}

	public void addJob(Job job) {
		jobs.put(job.getId(), job);
	}

	public void removeJob(int jobId) {
		jobs.remove(jobId);
	}

	public Job getJob(int jobId) {
		return jobs.get(jobId);
	}

	public int getNumJobs() {
		return jobs.size();
	}

	public ApplicationType getApplicationType() {
		return applicationType;
	}

	public void setApplicationType(ApplicationType applicationType) {
		this.applicationType = applicationType;
	}
	
	public void addFile(String filename) {
		fileList.add(filename);
	}
	
	public boolean removeFile(String filename) {
		return fileList.remove(filename);
	}
}
