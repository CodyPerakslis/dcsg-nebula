package edu.umn.cs.Nebula.request;

import java.util.ArrayList;

import edu.umn.cs.Nebula.application.JobType;

public class ComputeRequest {
	private JobType jobType;
	private ComputeRequestType requestType;
	private ArrayList<String> contents;
	
	public ComputeRequest(JobType jobType, ComputeRequestType requestType) {
		this.setJobType(jobType);
		this.setRequestType(requestType);
	}

	public JobType getJobType() {
		return jobType;
	}

	public void setJobType(JobType jobType) {
		this.jobType = jobType;
	}

	public ComputeRequestType getRequestType() {
		return requestType;
	}

	public void setRequestType(ComputeRequestType requestType) {
		this.requestType = requestType;
	}

	public ArrayList<String> getContents() {
		return contents;
	}

	public void setContents(ArrayList<String> contents) {
		this.contents = contents;
	}
}
