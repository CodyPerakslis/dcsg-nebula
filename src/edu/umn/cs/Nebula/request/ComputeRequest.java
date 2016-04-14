package edu.umn.cs.Nebula.request;

import java.util.ArrayList;

import edu.umn.cs.Nebula.model.JobType;

public class ComputeRequest {
	private String myIp;
	private String nodeIp;
	private JobType jobType;
	private ComputeRequestType requestType;
	private ArrayList<String> contents;
	private long timestamp;
	
	public ComputeRequest(String myIp, JobType jobType) {
		this.setMyIp(myIp);
		this.setJobType(jobType);
		requestType = null;
		contents = new ArrayList<String>();
	}
	
	public ComputeRequest(String myIp, JobType jobType, ComputeRequestType requestType) {
		this.setMyIp(myIp);
		this.setJobType(jobType);
		this.setRequestType(requestType);
		contents = new ArrayList<String>();
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
	
	public void addContent(String content) {
		contents.add(content);
	}
	
	public boolean removeContent(String content) {
		return contents.remove(content);
	}

	public String getNodeIp() {
		return nodeIp;
	}

	public void setNodeIp(String nodeIp) {
		this.nodeIp = nodeIp;
	}

	public String getMyIp() {
		return myIp;
	}

	public void setMyIp(String myIp) {
		this.myIp = myIp;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
}
