package edu.umn.cs.Nebula.model;

import java.util.Date;

public class Lease {
	private String scheduler;
	private Date expiredTime;
	
	public Lease(String scheduler, Date expiredTime) {
		this.scheduler = scheduler;
		this.setExpiredTime(expiredTime);
	}
	
	public Lease(String scheduler, long leaseTime) {
		this.scheduler = scheduler;
		Date expiry = new Date();
		expiry.setTime(expiry.getTime() + leaseTime);
		this.setExpiredTime(expiry);
	}

	public String getScheduler() {
		return scheduler;
	}
	
	public void setScheduler(String scheduler) {
		this.scheduler = scheduler;
	}

	public Date getExpiredTime() {
		return expiredTime;
	}

	public void setExpiredTime(Date expiredTime) {
		this.expiredTime = expiredTime;
	}
	
	public long getRemainingTime() {
		return new Date().getTime() - expiredTime.getTime();
	}
}
