package edu.umn.cs.Nebula.job;

import java.util.ArrayList;
import java.util.Date;

public class Task implements Comparable<Task> {
	private long taskId;
	private long jobId;
	private JobType type;
	private int priority;
	private TaskStatus status;
	
	private Date postedTime;
	private Date startTime;
	private Date completedTime;
	
	private String completingNode;
	private String inputFile;
	private String command;
	private String executableFile;
	private ArrayList<String> parameters;

	/* optional for tasks that belong to a specific location */
	private float latitude;
	private float longitude;
	
	public Task(long id, JobType type, TaskStatus status) {
		this.taskId = id;
		this.type = type;
		this.status = status;
	}
	
	public Task(long id, JobType type, String command, String executableFile, TaskStatus status) {
		this.taskId = id;
		this.type = type;
		this.status = status;
		this.command = command;
		this.executableFile = executableFile;
	}
	
	public Task(long id, long jobId, JobType type, String command, String executableFile, TaskStatus status) {
		this.taskId = id;
		this.jobId = jobId;
		this.type = type;
		this.priority = Job.DEFAULT_PRIORITY;
		this.status = status;
		this.postedTime = new Date();
		this.command = command;
		this.executableFile = executableFile;
		this.parameters = new ArrayList<String>();
	}
	
	public Task(long id, long jobId, JobType type, String inputFile, String command, String executableFile, TaskStatus status) {
		this.taskId = id;
		this.jobId = jobId;
		this.type = type;
		this.priority = Job.DEFAULT_PRIORITY;
		this.status = status;
		this.postedTime = new Date();
		this.inputFile = inputFile;
		this.command = command;
		this.executableFile = executableFile;
		this.parameters = new ArrayList<String>();
	}
	
	public long getId() {
		return taskId;
	}
	
	public void setId(long taskId) {
		this.taskId = taskId;
	}

	public long getJobId() {
		return jobId;
	}

	public void setJobId(long jobId) {
		this.jobId = jobId;
	}
	
	public JobType getType() {
		return type;
	}

	public void setType(JobType type) {
		this.type = type;
	}
	
	public TaskStatus getStatus() {
		return status;
	}

	public void setStatus(TaskStatus status) {
		this.status = status;
	}
	
	public int getPriority() {
		return priority;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}
	
	@Override
	public int compareTo(Task o) {
		return priority > o.priority ? 1 : -1;
	}

	@Override
	public boolean equals(Object o) {
		if(o instanceof Task){
			Task c = (Task) o;
			return taskId == c.getId();
		}
		return false;
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

	public void setCompletingNode(String nodeId) {
		completingNode = nodeId;
	}
	
	public String getCompletingNode() {
		return completingNode;
	}

	public String getCommand() {
		return command;
	}

	public void setCommand(String command) {
		this.command = command;
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
	
	public ArrayList<String> getParameters() {
		return parameters;
	}

	public void setParameters(ArrayList<String> param) {
		this.parameters = param;
	}

	public void addParameter(String param) {
		parameters.add(param);
	}
	
	public boolean removeParameter(String param) {
		return parameters.remove(param);
	}

	public float getLatitude() {
		return latitude;
	}

	public void setLatitude(float latitude) {
		this.latitude = latitude;
	}

	public float getLongitude() {
		return longitude;
	}

	public void setLongitude(float longitude) {
		this.longitude = longitude;
	}
}
