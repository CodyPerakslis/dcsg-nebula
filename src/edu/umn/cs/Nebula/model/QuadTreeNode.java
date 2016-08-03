package edu.umn.cs.Nebula.model;

import java.util.ArrayList;
import java.util.HashSet;

import edu.umn.cs.Nebula.node.NodeInfo;

public class QuadTreeNode extends NodeInfo {

	private static final long serialVersionUID = 1L;
	// basic tree attributes
	private QuadTreeNode parent;
	private QuadTreeNode NW;
	private QuadTreeNode NE;
	private QuadTreeNode SW;
	private QuadTreeNode SE;
	private double score;

	// specialized attributes
	public ArrayList<NodeInfo> coveredItems;
	public double topLatitude;
	public double bottomLatitude;
	public double leftLongitude;
	public double rightLongitude;

	public QuadTreeNode(double initScore, NodeInfo node) {
		super(node.getId(), node.getIp(), node.getLatitude(), node.getLongitude(), node.getNodeType());
		parent = null;
		NW = NE = SW = SE = null;
		score = initScore;
		coveredItems = new ArrayList<NodeInfo>();
		topLatitude = 90;
		bottomLatitude = -90;
		leftLongitude = -180;
		rightLongitude = 180;
	}

	public QuadTreeNode(double initScore, NodeInfo node, double topLatitude, double bottomLatitude, double leftLongitude, double rightLongitude) {
		super(node.getId(), node.getIp(), node.getLatitude(), node.getLongitude(), node.getNodeType());
		parent = null;
		NW = NE = SW = SE = null;
		score = initScore;
		coveredItems = new ArrayList<NodeInfo>();
		this.topLatitude = topLatitude;
		this.bottomLatitude = bottomLatitude;
		this.leftLongitude = leftLongitude;
		this.rightLongitude = rightLongitude;
	}

	public QuadTreeNode getParent() { return parent; }
	public QuadTreeNode getNW() { return NW; }
	public QuadTreeNode getNE() { return NE; }
	public QuadTreeNode getSW() { return SW; }
	public QuadTreeNode getSE() { return SE; }

	public void setParent(QuadTreeNode parent) { this.parent = parent; }

	public double setNW(QuadTreeNode nW) {
		HashSet<NodeInfo> temp = new HashSet<NodeInfo>();
		NW = nW; 
		for (NodeInfo item: coveredItems) {
			if (item.getLatitude() >= NW.bottomLatitude && item.getLongitude() < NW.rightLongitude) {
				NW.insertItem(item);
				temp.add(item);
			}
		}
		for (NodeInfo item: temp) {
			removeItem(item);
		}
		return score;
	}

	public double setNE(QuadTreeNode nE) {
		HashSet<NodeInfo> temp = new HashSet<NodeInfo>();
		NE = nE; 
		for (NodeInfo item: coveredItems) {
			if (item.getLatitude() >= NE.bottomLatitude && item.getLongitude() >= NE.leftLongitude) {
				NE.insertItem(item);
				temp.add(item);
			}
		}
		for (NodeInfo item: temp) {
			removeItem(item);
		}
		return score;
	}

	public double setSW(QuadTreeNode sW) { 
		HashSet<NodeInfo> temp = new HashSet<NodeInfo>();
		SW = sW; 
		for (NodeInfo item: coveredItems) {
			if (item.getLatitude() < SW.topLatitude && item.getLongitude() < SW.rightLongitude) {
				SW.insertItem(item);
				temp.add(item);
			}
		}
		for (NodeInfo item: temp) {
			removeItem(item);
		}
		return score;
	}

	public double setSE(QuadTreeNode sE) {
		HashSet<NodeInfo> temp = new HashSet<NodeInfo>();
		SE = sE; 
		for (NodeInfo item: coveredItems) {
			if (item.getLatitude() < SE.topLatitude && item.getLongitude() >= SE.leftLongitude) {
				SE.insertItem(item);
				temp.add(item);
			}
		}
		for (NodeInfo item: temp) {
			removeItem(item);
		}
		return score;
	}

	public double getScore() { return score; }
	public void setScore(double score) { this.score = score; }

	/* Helper functions for accessing the items (only for leaf nodes) */
	public boolean insertItem(NodeInfo item) { 
		score = getInsertNewScore(item);
		return coveredItems.add(item); 
	}
	public boolean removeItem(NodeInfo item) {
		score = getRemoveNewScore(item);
		return coveredItems.remove(item); 
	}
	public int getNumNodes() { return coveredItems.size(); }
	public boolean hasItem(NodeInfo item) { return coveredItems.contains(item); }

	public QuadTreeNode getQuadrant(double latitude, double longitude) {
		if (latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180)
			return null;
		// find the corresponding child node
		if (latitude < (topLatitude + bottomLatitude)/2) {
			if (longitude < (leftLongitude + rightLongitude)/2) return SW; 	// South West Quadrant		
			else return SE; 												// South East Quadrant	
		} else {
			if (longitude < (leftLongitude + rightLongitude)/2) return NW; 	// North West Quadrant
			else return NE; 												// North East Quadrant		
		}
	}

	/**
	 * TODO implement the score updating function for adding an item into the tree
	 * @return the updated score after insertion
	 */
	public double getInsertNewScore(NodeInfo item) {
		return score + 1;
	}

	/**
	 * TODO implement the score updating function for removing an item from the tree
	 * @return the updated score after removal
	 */
	public double getRemoveNewScore(NodeInfo item) {
		return score - 1;
	}

}
