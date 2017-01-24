package edu.umn.cs.Nebula.job;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

public class Job implements Comparable<Job> {
	public final static int DEFAULT_PRIORITY = 5;
	
	private long jobId;
	private long applicationId;
	private JobType type;
	private int priority;
	private boolean isActive;
	private boolean isCompleted;
	private ArrayList<Long> dependencies;
	
	private Date postedTime;
	private Date startTime;
	private Date completedTime;
	
	private ArrayList<String> inputFiles;
	private String command;
	private String executableFile;
	private ArrayList<String> parameters;
	
	private HashMap<Long, Task> tasks;
	
	public Job(int id, int applicationId, JobType type, String command, String executableFile, TaskStatus status) {
		this.jobId = id;
		this.applicationId = applicationId;
		this.priority = Job.DEFAULT_PRIORITY;
		this.type = type;
		this.isActive = true;
		this.isCompleted = false;
		this.dependencies = new ArrayList<Long>();
		this.postedTime = new Date();
		this.inputFiles = new ArrayList<String>();
		this.command = command;
		this.executableFile = executableFile;
		this.parameters = new ArrayList<String>();
		this.tasks = new HashMap<Long, Task>();
	}

	public Job(int id, int applicationId, int priority, JobType type, String command, String executableFile, TaskStatus status) {
		this.jobId = id;
		this.applicationId = applicationId;
		this.priority = priority;
		this.type = type;
		this.isActive = true;
		this.isCompleted = false;
		this.dependencies = new ArrayList<Long>();
		this.postedTime = new Date();
		this.inputFiles = new ArrayList<String>();
		this.command = command;
		this.executableFile = executableFile;
		this.parameters = new ArrayList<String>();
		this.tasks = new HashMap<Long, Task>();
	}
	
	public long getId() {
		return jobId;
	}
	
	public void setId(long id) {
		this.jobId = id;
	}

	public long getApplicationId() {
		return applicationId;
	}
	
	public void setApplicationId(long id) {
		this.applicationId = id;
	}
	
	public JobType getJobType() {
		return type;
	}

	public void setJobType(JobType jobType) {
		this.type = jobType;
	}
	
	public int getPriority() {
		return priority;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}
	
	@Override
	public int compareTo(Job o) {
		return priority > o.priority ? 1 : -1;
	}

	@Override
	public boolean equals(Object o) {
		if(o instanceof Job){
			Job c = (Job) o;
			return jobId == c.getId();
		}
		return false;
	}
	
	public void setActive(boolean active) {
		this.isActive = active;
	}

	public boolean isActive() {
		return isActive;
	}

	public void setComplete(boolean complete) {
		completedTime = new Date();
		this.isCompleted = complete;
	}

	public boolean isComplete() {
		return isCompleted;
	}
	
	public ArrayList<Long> getDependencies() {
		return dependencies;
	}
	
	public int addDependency(long jobId) {
		dependencies.add(jobId);
		return dependencies.size();
	}
	
	public int removeDependency(long jobId) {
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

	public String getCommand() {
		return command;
	}

	public void setCommand(String command) {
		this.command = command;
	}
	
	public ArrayList<String> getInputFiles() {
		return inputFiles;
	}

	public void setInputFiles(ArrayList<String> inputFiles) {
		this.inputFiles = inputFiles;
	}

	public int addInputFile(String inputFile) {
		inputFiles.add(inputFile);
		return inputFiles.size();
	}
	
	public int removeInputFile(String inputFile) {
		inputFiles.remove(inputFile);
		return inputFiles.size();
	}
	public String getExecutableFile() {
		return executableFile;
	}

	public void setExecutableFile(String executableFile) {
		this.executableFile = executableFile;
	}
	
	public ArrayList<String> getParameters() {
		return parameters;
	}

	public void setParameters(ArrayList<String> param) {
		this.parameters = param;
	}

	public int addParameter(String param) {
		parameters.add(param);
		return parameters.size();
	}
	
	public int removeParameter(String param) {
		parameters.remove(param);
		return parameters.size();
	}
	
	public HashMap<Long, Task> getTasks() {
		return tasks;
	}
	
	public void setTasks(HashMap<Long, Task> tasks) {
		this.tasks = tasks;
	}

	public int addTask(Task task) {
		tasks.put(task.getId(), task);
		return tasks.size();
	}

	public int removeTask(int taskId) {
		tasks.remove(taskId);
		return tasks.size();
	}

	public Task getTask(int taskId) {
		return tasks.get(taskId);
	}

	public int getNumTasks() {
		return tasks.size();
	}
}
