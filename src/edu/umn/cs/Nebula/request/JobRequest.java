package edu.umn.cs.Nebula.request;

import edu.umn.cs.Nebula.job.Job;

public class JobRequest {
	private Job job;
	private JobRequestType type;
	
	public JobRequest(Job job, JobRequestType type) {
		setJob(job);
		setType(type);
	}
	
	public Job getJob() {
		return job;
	}
	
	public void setJob(Job job) {
		this.job = job;
	}

	public JobRequestType getType() {
		return type;
	}

	public void setType(JobRequestType type) {
		this.type = type;
	}
}
