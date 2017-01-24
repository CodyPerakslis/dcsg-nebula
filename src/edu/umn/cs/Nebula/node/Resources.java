package edu.umn.cs.Nebula.node;

public class Resources {
	private Runtime r = Runtime.getRuntime();
	private int numCPUs;
	private long totalAvailableMemory;
	private long freeMemory;

	public Resources(int numCPUs, long totalAvailableMemory) {
		this.setNumCPUs(numCPUs);
		this.setTotalAvailableMemory(totalAvailableMemory);
	}
	
	public Resources() {
		numCPUs = r.availableProcessors();
		totalAvailableMemory = r.totalMemory();
		freeMemory = r.freeMemory();
	}

	public int getNumCPUs() {
		return numCPUs;
	}

	public void setNumCPUs(int numCPUs) {
		if (numCPUs > r.availableProcessors()) {
			this.numCPUs = r.availableProcessors();
		} else {
			this.numCPUs = numCPUs;
		}
	}
	
	public long getTotalAvailableMemory() {
		return totalAvailableMemory;
	}

	public void setTotalAvailableMemory(long totalAvailableMemory) {
		if (totalAvailableMemory > r.totalMemory()) {
			this.totalAvailableMemory = r.totalMemory();
		} else {
			this.totalAvailableMemory = totalAvailableMemory;
		}
	}

	public long getFreeMemory() {
		return freeMemory;
	}
	
	public String getResourcesInfo() {
		return "\tCPU: " + numCPUs + "\n" 
				+ "\tMemory: " + (freeMemory/(1024*1024)) + "/" + (totalAvailableMemory/(1024*1024)) + " MB";
	}
}
