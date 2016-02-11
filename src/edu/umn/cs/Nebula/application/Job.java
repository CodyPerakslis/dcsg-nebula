package edu.umn.cs.Nebula.application;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

public class Job {
	private int id;
	private int applicationId;
	private JobType jobType;
	private int priority;
	private int numNodes;
	private boolean active;
	private boolean complete;
	private ArrayList<Integer> dependencies;
	private Date postedTime;
	private Date startTime;
	private Date completedTime;
	
	private String exeFile;
	private ArrayList<String> fileList;
	
	private HashMap<Integer, Task> tasks;
	
	public Job(int id, int applicationId, int priority, JobType type) {
		this.id = id;
		this.applicationId = applicationId;
		this.setPriority(priority);
		this.jobType = type;
		active = true;
		complete = false;
		postedTime = new Date();
		dependencies = new ArrayList<Integer>();
		tasks = new HashMap<Integer, Task>();
		fileList = new ArrayList<String>();
	}

	public Job(int id, int applicationId, int priority, JobType type, HashMap<Integer, Task> tasks) {
		this.id = id;
		this.applicationId = applicationId;
		this.jobType = type;
		active = true;
		complete = false;
		postedTime = new Date();
		dependencies = new ArrayList<Integer>();
		this.tasks = tasks;
		fileList = new ArrayList<String>();
	}
	
	public int getId() {
		return id;
	}

	public int getApplicationId() {
		return applicationId;
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
	
	public ArrayList<Integer> getDependencies() {
		return dependencies;
	}
	
	public int addDependency(int jobId) {
		dependencies.add(jobId);
		return dependencies.size();
	}
	
	public int removeDependency(int jobId) {
		if (dependencies.contains(jobId)) {
			dependencies.remove(jobId);
		}
		return dependencies.size();
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

	public HashMap<Integer, Task> getTasks() {
		return tasks;
	}
	
	public void setExeFile(String exeFile) {
		this.exeFile = exeFile;
	}
	
	public String getExeFile() {
		return exeFile;
	}
	
	public void setTasks(HashMap<Integer, Task> tasks) {
		this.tasks = tasks;
	}

	public void addTask(Task task) {
		tasks.put(task.getId(), task);
	}

	public void removeTask(int taskId) {
		tasks.remove(taskId);
	}

	public Task getTask(int taskId) {
		return tasks.get(taskId);
	}

	public int getNumTasks() {
		return tasks.size();
	}

	public JobType getJobType() {
		return jobType;
	}

	public void setJobType(JobType jobType) {
		this.jobType = jobType;
	}
	
	public void setFileList(ArrayList<String> fileList) {
		this.fileList = fileList;
	}
	
	public ArrayList<String> getFileList() {
		return fileList;
	}
	
	public int getNumFiles() {
		return fileList.size();
	}
	
	public void addFile(String filename) {
		fileList.add(filename);
	}
	
	public boolean removeFile(String filename) {
		return fileList.remove(filename);
	}

	public int getNumNodes() {
		return numNodes;
	}

	public void setNumNodes(int numNodes) {
		this.numNodes = numNodes;
	}

	public int getPriority() {
		return priority;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}
}
