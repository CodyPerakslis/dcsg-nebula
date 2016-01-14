package edu.umn.cs.MCC.model;

import java.io.Serializable;
import java.util.Date;

public class Node implements Serializable {
	private static final long serialVersionUID = 4895419655829174912L;
	private String id;
	private String ip;
	private double latitude;
	private double longitude;
	private double bandwidth;
	private double latency;
	private NodeType nodeType;
	private Date lastOnline;
	
	public Node(String id, String ip, double latitude, double longitude, NodeType nodeType) {
		this.id = id;
		this.ip = ip;
		this.latitude = latitude;
		this.longitude = longitude;
		this.nodeType = nodeType;
		this.lastOnline = new Date();
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	public String getId() {
		return id;
	}
	
	public void setIp(String ip) {
		this.ip = ip;
	}
	
	public String getIp() {
		return ip;
	}
	
	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}
	
	public double getLatitude() {
		return latitude;
	}
	
	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}
	
	public double getLongitude() {
		return longitude;
	}
	
	public void setBandwidth(double bandwidth) {
		this.bandwidth = bandwidth;
	}
	
	public double getBandwidth() {
		return bandwidth;
	}
	
	public void setNodeType(NodeType nodeType) {
		this.nodeType = nodeType;
	}
	
	public NodeType getNodeType() {
		return nodeType;
	}
	
	public void updateLastOnline() {
		this.lastOnline = new Date();
	}
	
	public Date getLastOnline() {
		return lastOnline;
	}

	public double getLatency() {
		return latency;
	}

	public void setLatency(double latency) {
		this.latency = latency;
	}
}
