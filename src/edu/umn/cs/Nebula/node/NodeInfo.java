package edu.umn.cs.Nebula.node;

import java.io.Serializable;
import java.util.LinkedList;

public class NodeInfo implements Serializable {
	private static final long serialVersionUID = 4895419655829174912L;
	private int maxRecord = 10;
	private String id;
	private String ip;
	private float latitude;
	private float longitude;
	private LinkedList<Double> bandwidth;
	private LinkedList<Double> latency;
	private NodeType nodeType;
	private long lastOnline;
	private Resources resources;
	private String note;
	
	public NodeInfo(String id, String ip, float latitude, float longitude, NodeType nodeType) {
		this.id = id;
		this.ip = ip;
		this.latitude = latitude;
		this.longitude = longitude;
		this.nodeType = nodeType;
		this.lastOnline = System.currentTimeMillis();
		this.bandwidth = new LinkedList<Double>();
		this.latency = new LinkedList<Double>();
		this.setResources(new Resources());
	}
	
	public NodeInfo(String id, String ip, float latitude, float longitude, NodeType nodeType, double bw, double lt) {
		this.id = id;
		this.ip = ip;
		this.latitude = latitude;
		this.longitude = longitude;
		this.nodeType = nodeType;
		this.lastOnline = System.currentTimeMillis();
		this.bandwidth = new LinkedList<Double>();
		if (bw > 0) {
			addBandwidth(bw);
		}
		this.latency = new LinkedList<Double>();
		if (lt > 0) {
			addLatency(lt);
		}
	}
	
	public NodeInfo(String id, String ip, NodeType nodeType) {
		this.id = id;
		this.ip = ip;
		this.nodeType = nodeType;
		this.lastOnline = System.currentTimeMillis();
		this.bandwidth = new LinkedList<Double>();
		this.latency = new LinkedList<Double>();
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
	
	public void setLatitude(float latitude) {
		this.latitude = latitude;
	}
	
	public float getLatitude() {
		return latitude;
	}
	
	public void setLongitude(float longitude) {
		this.longitude = longitude;
	}
	
	public float getLongitude() {
		return longitude;
	}
	
	public void addBandwidth(double bandwidth) {
		if (bandwidth <= 0) {
			return;
		}
		if (this.bandwidth.size() >= maxRecord) {
			this.bandwidth.removeFirst();
		}
		this.bandwidth.addLast(bandwidth);
	}
	
	public double getBandwidth() {
		if (bandwidth.isEmpty()) {
			return -1;
		} else {
			int i = 0;
			double total = 0;
			while (i < maxRecord && i < bandwidth.size()) {
				total++;
				i++;
			}
			return total/i;
		}
	}
	
	public void setNodeType(NodeType nodeType) {
		this.nodeType = nodeType;
	}
	
	public NodeType getNodeType() {
		return nodeType;
	}
	
	public void updateLastOnline() {
		this.lastOnline = System.currentTimeMillis();
	}
	
	public long getLastOnline() {
		return lastOnline;
	}

	public void addLatency(double latency) {
		if (latency <= 0) {
			return;
		}
		if (this.latency.size() >= maxRecord) {
			this.latency.removeFirst();
		}
		this.latency.addLast(latency);
	}
	
	public double getLatency() {
		if (latency.isEmpty()) {
			return -1;
		} else {
			int i = 0;
			double total = 0;
			while (i < maxRecord && i < latency.size()) {
				total++;
				i++;
			}
			return total/i;
		}
	}

	public String getNote() {
		return note;
	}

	public void setNote(String note) {
		this.note = note;
	}

	public Resources getResources() {
		return resources;
	}

	public void setResources(Resources resources) {
		this.resources = resources;
	}
}
