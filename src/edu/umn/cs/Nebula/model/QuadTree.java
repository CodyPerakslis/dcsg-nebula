package edu.umn.cs.Nebula.model;

import java.util.ArrayList;

public class QuadTree {
	private QuadTreeNode root;
	private double splitThreshold;
	private double mergeThreshold;
	private double initialScore;
	
	public ArrayList<NodeInfo> availableNodes;

	public QuadTree(double initScore, double splitThreshold, double mergeThreshold, NodeInfo node) {
		initialScore = initScore;
		availableNodes = new ArrayList<NodeInfo>();
		this.setSplitThreshold(splitThreshold);
		this.setMergeThreshold(mergeThreshold);
		setRoot(new QuadTreeNode(initialScore, node));
	}

	public QuadTreeNode getRoot() { return root; }
	public void setRoot(QuadTreeNode root) { this.root = root; }

	public double getSplitThreshold() { return splitThreshold; }
	public double getMergeThreshold() { return mergeThreshold; }
	public void setSplitThreshold(double splitThreshold) { this.splitThreshold = splitThreshold; }
	public void setMergeThreshold(double mergeThreshold) { this.mergeThreshold = mergeThreshold; }

	/**
	 * Insert a QuadTreeItem into the tree starting from the root.
	 * 
	 * @param item	the item to be inserted
	 * @return	the node to which the item is inserted into, null if failed
	 */
	public NodeInfo insertItem(NodeInfo item) {
		if (item.getLatitude() < -90 || item.getLatitude() > 90 || item.getLongitude() < -180 || item.getLongitude() > 180)
			return null;

		// find the corresponding child node
		QuadTreeNode treeNode = root, temp = root;
		while ((treeNode = treeNode.getQuadrant(item.getLatitude(), item.getLongitude())) != null) {
			temp = treeNode;
		}
		treeNode = temp;

		double newScore = treeNode.getInsertNewScore(item);
		if (newScore > splitThreshold && !availableNodes.isEmpty()) {			
			NodeInfo newNode = null;
			
			for (NodeInfo node: availableNodes) {
				// TODO need a better algorithm to select nodes to avoid adding a new node to a non-busy quadrant
				if (node.getLatitude() <= treeNode.topLatitude && node.getLatitude() >= treeNode.bottomLatitude && 
						node.getLongitude() <= treeNode.rightLongitude && node.getLongitude() >= treeNode.leftLongitude) {
					newNode = node;
					break;
				}
			}
			
			treeNode.insertItem(item);
			if (newNode != null) {
				// set a new node as a child of the treeNode and distribute the items among them
				QuadTreeNode newTreeNode = new QuadTreeNode(initialScore, newNode);
				newTreeNode.setParent(treeNode);
				
				if (newNode.getLatitude() < (treeNode.topLatitude + treeNode.bottomLatitude)/2) {
					if (newNode.getLongitude() < (treeNode.leftLongitude + treeNode.rightLongitude)/2) {
						newTreeNode.topLatitude = (treeNode.topLatitude + treeNode.bottomLatitude)/2;
						newTreeNode.bottomLatitude = treeNode.bottomLatitude;
						newTreeNode.leftLongitude = treeNode.leftLongitude;
						newTreeNode.rightLongitude = (treeNode.leftLongitude + treeNode.rightLongitude)/2;
						treeNode.setSW(newTreeNode);
					} else { 
						newTreeNode.topLatitude = (treeNode.topLatitude + treeNode.bottomLatitude)/2;
						newTreeNode.bottomLatitude = treeNode.bottomLatitude;
						newTreeNode.leftLongitude = (treeNode.leftLongitude + treeNode.rightLongitude)/2;
						newTreeNode.rightLongitude = treeNode.rightLongitude;
						treeNode.setSE(newTreeNode);
					}
				} else {
					if (newNode.getLongitude() < (treeNode.leftLongitude + treeNode.rightLongitude)/2) {
						newTreeNode.topLatitude = treeNode.topLatitude;
						newTreeNode.bottomLatitude = (treeNode.topLatitude + treeNode.bottomLatitude)/2;
						newTreeNode.leftLongitude = treeNode.leftLongitude;
						newTreeNode.rightLongitude = (treeNode.leftLongitude + treeNode.rightLongitude)/2;
						treeNode.setNW(newTreeNode);
					} else {
						newTreeNode.topLatitude = treeNode.topLatitude;
						newTreeNode.bottomLatitude = (treeNode.topLatitude + treeNode.bottomLatitude)/2;
						newTreeNode.leftLongitude = (treeNode.leftLongitude + treeNode.rightLongitude)/2;
						newTreeNode.rightLongitude = treeNode.rightLongitude;
						treeNode.setNE(newTreeNode);	
					}
				}
				availableNodes.remove(newNode);
			}
		} else {
			treeNode.insertItem(item);
		}
		return treeNode;
	}

	/**
	 * Recursively find an item from the tree and remove it.
	 * 
	 * @param item
	 * @return whether the item is found and has been removed
	 */
	public boolean remove(NodeInfo item) {
		QuadTreeNode treeNode = root;

		do {
			if (treeNode.hasItem(item)) {
				return treeNode.removeItem(item);
			}
			treeNode = treeNode.getQuadrant(item.getLatitude(), item.getLongitude());
		} while (treeNode != null);

		return false;
	}
}
