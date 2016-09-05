package edu.umn.cs.Nebula.request;

import edu.umn.cs.Nebula.model.NodeInfo;

public class MobileRequest {
	private NodeInfo node;
	private MobileRequestType type;
	
	public MobileRequest(MobileRequestType type) {
		this.type = type;
	}
	
	public MobileRequest(NodeInfo node, MobileRequestType type) {
		this.node = node;
		this.type = type;
	}
	
	public NodeInfo getNode() {
		return node;
	}
	
	public void setNode(NodeInfo node) {
		this.node = node;
	}

	public MobileRequestType getType() {
		return type;
	}

	public void setType(MobileRequestType type) {
		this.type = type;
	}
}
