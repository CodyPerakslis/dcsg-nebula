package edu.umn.cs.MCC.model;

public class NodeRequest {
	private Node node;
	private NodeRequestType type;
	
	public NodeRequest(Node node, NodeRequestType type) {
		this.node = node;
		this.type = type;
	}
	
	public Node getNode() {
		return node;
	}
	
	public void setNode(Node node) {
		this.node = node;
	}

	public NodeRequestType getType() {
		return type;
	}

	public void setType(NodeRequestType type) {
		this.type = type;
	}
}
