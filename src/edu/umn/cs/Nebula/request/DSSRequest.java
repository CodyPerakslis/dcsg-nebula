package edu.umn.cs.Nebula.request;

public class DSSRequest {
	private String nodeId;
	private DSSRequestType type;
	private String namespace;
	private String filename;
	private float latitude;
	private float longitude;
	private String note;
	
	public DSSRequest(DSSRequestType type) {
		this.type = type;
	}
	
	public DSSRequest(DSSRequestType type, String namespace, String filename) {
		this.type = type;
		this.namespace = namespace;
		this.filename = filename;
	}

	public DSSRequest(String id, DSSRequestType type, String namespace, String filename) {
		this.nodeId = id;
		this.type = type;
		this.namespace = namespace;
		this.filename = filename;
	}

	
	public DSSRequestType getType() {
		return type;
	}

	public void setType(DSSRequestType type) {
		this.type = type;
	}

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public float getLatitude() {
		return latitude;
	}

	public void setLatitude(float latitude) {
		this.latitude = latitude;
	}

	public float getLongitude() {
		return longitude;
	}

	public void setLongitude(float longitude) {
		this.longitude = longitude;
	}

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public String getNote() {
		return note;
	}

	public void setNote(String note) {
		this.note = note;
	}
	
	
}
