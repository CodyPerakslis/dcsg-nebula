package edu.umn.cs.Nebula.request;

import edu.umn.cs.Nebula.node.NodeType;

public class SchedulerRequest {
	private SchedulerRequestType type;
	private NodeType nodeType;

	public SchedulerRequest(SchedulerRequestType type, NodeType nodeType) {
		this.type = type;
		this.setNodeType(nodeType);
	}
	
	public SchedulerRequestType getType() {
		return type;
	}

	public void setType(SchedulerRequestType type) {
		this.type = type;
	}

	public NodeType getNodeType() {
		return nodeType;
	}

	public void setNodeType(NodeType nodeType) {
		this.nodeType = nodeType;
	}
}
