package edu.umn.cs.Nebula.request;

import java.util.ArrayList;
import java.util.HashMap;

import edu.umn.cs.Nebula.model.ApplicationRequestType;
import edu.umn.cs.Nebula.model.ApplicationType;
import edu.umn.cs.Nebula.model.JobType;

public class ApplicationRequest {
	private ApplicationRequestType type;
	private String applicationId;
	private String applicationName;
	private ApplicationType applicationType;
	private ArrayList<String> fileList;
	private HashMap<JobType, String> jobExecutable;
	private HashMap<JobType, ArrayList<String>> jobInputs;
	
	public ApplicationRequest(ApplicationRequestType type, String applicationName, ApplicationType applicationType) {
		this.setType(type);
		this.applicationName = applicationName;
		this.setApplicationType(applicationType);
		fileList = new ArrayList<String>();
		jobExecutable = new HashMap<JobType, String>();
		jobInputs = new HashMap<JobType, ArrayList<String>>();
	}
	
	public ApplicationRequest(ApplicationRequestType type, String applicationId, String applicationName, ApplicationType applicationType) {
		this.setType(type);
		this.applicationId = applicationId;
		this.applicationName = applicationName;
		this.setApplicationType(applicationType);
		fileList = new ArrayList<String>();
		jobExecutable = new HashMap<JobType, String>();
		jobInputs = new HashMap<JobType, ArrayList<String>>();
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

	public ArrayList<String> getFileList() {
		return fileList;
	}

	public void setFileList(ArrayList<String> fileList) {
		this.fileList = fileList;
	}
	
	public void addFile(String filename) {
		fileList.add(filename);
	}
	
	public int removeFile(String filename) {
		fileList.remove(filename);
		return fileList.size();
	}

	public ApplicationRequestType getType() {
		return type;
	}

	public void setType(ApplicationRequestType type) {
		this.type = type;
	}

	public String getApplicationId() {
		return applicationId;
	}

	public void setApplicationId(String applicationId) {
		this.applicationId = applicationId;
	}

	public String getJobExecutable(JobType jobType) {
		return jobExecutable.get(jobType);
	}

	public void addJobExecutable(JobType jobType, String filename) {
		jobExecutable.put(jobType, filename);
	}

	public ArrayList<String> getJobInputs(JobType jobType) {
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
