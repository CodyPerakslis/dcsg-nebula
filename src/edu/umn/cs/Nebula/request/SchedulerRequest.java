package edu.umn.cs.Nebula.request;

import java.util.HashMap;
import java.util.Set;

import edu.umn.cs.Nebula.model.Lease;


public class SchedulerRequest {
	private SchedulerRequestType type;
	private String schedulerName;
	private HashMap<String, Lease> leases;

	public SchedulerRequest(SchedulerRequestType type, String schedulerName) {
		this.type = type;
		this.setSchedulerName(schedulerName);
		leases = new HashMap<String, Lease>();
	}
	
	public SchedulerRequestType getType() {
		return type;
	}

	public void setType(SchedulerRequestType type) {
		this.type = type;
	}

	public String getSchedulerName() {
		return schedulerName;
	}

	public void setSchedulerName(String schedulerName) {
		this.schedulerName = schedulerName;
	}
	
	public void addLease(String nodeId, Lease lease) {
		leases.put(nodeId, lease);
	}
	
	public Lease removeLease(String nodeId) {
		return leases.remove(nodeId);
	}
	
	public Lease getLease(String nodeId) {
		return leases.get(nodeId);
	}
	
	public int getNumLeases() {
		return leases.size();
	}
	
	public Set<String> getNodes() {
		return leases.keySet();
	}
}
