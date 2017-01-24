package edu.umn.cs.Nebula.schedule;


public class Lease {
	private String scheduler;
	private long expiredTime;
	
	public Lease(String scheduler, long leaseTime) {
		this.scheduler = scheduler;
		this.expiredTime = System.currentTimeMillis() + leaseTime;
	}

	public String getScheduler() {
		return scheduler;
	}
	
	public void setScheduler(String scheduler) {
		this.scheduler = scheduler;
	}

	public long getExpiredTime() {
		return expiredTime;
	}

	public void setExpiredTime(long expiredTime) {
		this.expiredTime = expiredTime;
	}
	
	public long extendLease(long time) {
		expiredTime += time;
		return expiredTime;
	}
	
	public long getRemainingTime() {
		return expiredTime - System.currentTimeMillis();
	}
}
