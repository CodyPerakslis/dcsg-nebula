package edu.umn.cs.MCC.request;

import edu.umn.cs.MCC.node.NodeInfo;

public class NodeRequest {
	private NodeInfo node;
	private NodeRequestType type;
	
	public NodeRequest(NodeInfo node, NodeRequestType type) {
		this.node = node;
		this.type = type;
	}
	
	public NodeInfo getNode() {
		return node;
	}
	
	public void setNode(NodeInfo node) {
		this.node = node;
	}

	public NodeRequestType getType() {
		return type;
	}

	public void setType(NodeRequestType type) {
		this.type = type;
	}
}
