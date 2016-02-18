package edu.umn.cs.Nebula.request;

import java.util.ArrayList;
import java.util.HashMap;

import edu.umn.cs.Nebula.model.ApplicationType;
import edu.umn.cs.Nebula.model.JobType;

public class ApplicationRequest {
	private ApplicationRequestType type;
	private int applicationId;
	private String applicationName;
	private ApplicationType applicationType;
	private HashMap<JobType, String> jobExecutable;
	private HashMap<JobType, ArrayList<String>> jobInputs;
	private HashMap<JobType, Integer> jobWorkers;
	
	public ApplicationRequest(ApplicationRequestType type, ApplicationType applicationType) {
		this.setType(type);
		this.setApplicationType(applicationType);
	}
	
	public ApplicationRequest(ApplicationRequestType type, int id, ApplicationType applicationType) {
		this.setType(type);
		this.applicationId = id;
		this.setApplicationType(applicationType);
	}
	
	public ApplicationRequest(ApplicationRequestType type, String applicationName, ApplicationType applicationType) {
		this.setType(type);
		this.applicationName = applicationName;
		this.setApplicationType(applicationType);
		jobExecutable = new HashMap<JobType, String>();
		jobInputs = new HashMap<JobType, ArrayList<String>>();
		jobWorkers = new HashMap<JobType, Integer>();
	}
	
	public ApplicationRequest(ApplicationRequestType type, int applicationId, String applicationName, ApplicationType applicationType) {
		this.setType(type);
		this.applicationId = applicationId;
		this.applicationName = applicationName;
		this.setApplicationType(applicationType);
		jobExecutable = new HashMap<JobType, String>();
		jobInputs = new HashMap<JobType, ArrayList<String>>();
		jobWorkers = new HashMap<JobType, Integer>();
	}
	
	public String getApplicationName() {
		return applicationName;
	}
	
	public void setApplicationName(String applicationName) {
		this.applicationName = applicationName;
	}

	public ApplicationType getApplicationType() {
		return applicationType;
	}

	public void setApplicationType(ApplicationType applicationType) {
		this.applicationType = applicationType;
	}

	public void addFile(JobType type, String filename) {
		jobInputs.get(type).add(filename);
	}
	
	public int removeFile(JobType type, String filename) {
		jobInputs.get(type).remove(filename);
		return jobInputs.get(type).size();
	}

	public void setWorkers(JobType type, int numWorkers) {
		jobWorkers.put(type, numWorkers);
	}
	
	public int getNumWorkers(JobType type) {
		if (!jobWorkers.containsKey(type)) {
			return -1;
		}
		return jobWorkers.get(type);
	}
	
	public void clearWorkers(JobType type) {
		jobWorkers.clear();
	}
	
	public ApplicationRequestType getType() {
		return type;
	}

	public void setType(ApplicationRequestType type) {
		this.type = type;
	}

	public int getApplicationId() {
		return applicationId;
	}

	public void setApplicationId(int applicationId) {
		this.applicationId = applicationId;
	}

	public String getJobExecutable(JobType jobType) {
		return jobExecutable.get(jobType);
	}

	public void addJobExecutable(JobType jobType, String filename) {
		jobExecutable.put(jobType, filename);
	}

	public ArrayList<String> getJobInputs(JobType jobType) {
		if (!jobInputs.containsKey(jobType)) {
			return new ArrayList<String>();
		}
		return jobInputs.get(jobType);
	}

	public void addJobInput(JobType jobType, String input) {
		if (!jobInputs.containsKey(jobType)) {
			ArrayList<String> inputs = new ArrayList<String>();
			jobInputs.put(jobType, inputs);
		}
		jobInputs.get(jobType).add(input);
	}
}
