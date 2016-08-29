package edu.umn.cs.Nebula.model;

import java.util.ArrayList;

public class QuadTree {
	private QuadTreeNode root;
	private double splitThreshold;
	private double mergeThreshold;
	private double initialScore;
	
	public ArrayList<NodeInfo> availableNodes;

	public QuadTree(double initScore, double splitThreshold, double mergeThreshold, NodeInfo root) {
		initialScore = initScore;
		availableNodes = new ArrayList<NodeInfo>();
		this.setSplitThreshold(splitThreshold);
		this.setMergeThreshold(mergeThreshold);
		setRoot(new QuadTreeNode(initialScore, root));
	}

	public QuadTreeNode getRoot() { return root; }
	public void setRoot(QuadTreeNode root) { this.root = root; }
	
	public double getSplitThreshold() { return splitThreshold; }
	public double getMergeThreshold() { return mergeThreshold; }
	public void setSplitThreshold(double splitThreshold) { this.splitThreshold = splitThreshold; }
	public void setMergeThreshold(double mergeThreshold) { this.mergeThreshold = mergeThreshold; }

	/**
	 * Insert a client into the tree starting from the root.
	 * 
	 * @param 	client		the client to be inserted
	 * @return	the node to which the client is inserted into, null if failed
	 */
	public NodeInfo insertItem(NodeInfo client) {
		// make sure that the client's coordinate is valid
		if (client.getLatitude() < -90 || client.getLatitude() > 90 || client.getLongitude() < -180 || client.getLongitude() > 180)
			return null;

		// find the corresponding node
		QuadTreeNode treeNode = root, temp = root;
		while ((treeNode = treeNode.getQuadrant(client.getLatitude(), client.getLongitude())) != null) {
			// the treeNode has a child node that covers the client's quadrant
			temp = treeNode;
		}
		treeNode = temp;	// set the treeNode to be the leaf node covering the client's quadrant

		/*
		double newScore = treeNode.getInsertNewScore(client);
		if (newScore > splitThreshold && !availableNodes.isEmpty()) {
			// insertion of a new client raise the load to be > the split threshold
			// find a new node to distribute the load
			NodeInfo newNode = null;
			Coordinate clientsCoor = treeNode.getMeanClientsCoordinate();
			for (NodeInfo node: availableNodes) {
				// only find nodes within the coverage quadrant
				if (node.getLatitude() <= treeNode.topLatitude && node.getLatitude() >= treeNode.bottomLatitude && 
						node.getLongitude() <= treeNode.rightLongitude && node.getLongitude() >= treeNode.leftLongitude) {
					// find the closest node to the mean clients' coordinate
					if (newNode == null || // the node is closer in geographic distance to the mean clients' coordinate than the newNode 
							((Math.abs(node.getLatitude() - clientsCoor.getLatitude()) + Math.abs(node.getLongitude() - clientsCoor.getLongitude())) <
							(Math.abs(newNode.getLatitude() - clientsCoor.getLatitude()) + Math.abs(newNode.getLongitude() - clientsCoor.getLongitude())))) {
						newNode = node;
					}
				}
			}
			
			treeNode.insertClient(client);
			// if there is no other available node in the region covered by the current node, overload the current node
			// ALTERNATIVE: share the load with neighboring nodes
			if (newNode != null) {
				// set a new node as a child of the treeNode and distribute the items among them
				QuadTreeNode newTreeNode = new QuadTreeNode(initialScore, newNode);
				newTreeNode.setParent(treeNode);
				
				if (newNode.getLatitude() < (treeNode.topLatitude + treeNode.bottomLatitude)/2) { // South Quadrant
					if (newNode.getLongitude() < (treeNode.leftLongitude + treeNode.rightLongitude)/2) {
						newTreeNode.topLatitude = (treeNode.topLatitude + treeNode.bottomLatitude)/2;
						newTreeNode.bottomLatitude = treeNode.bottomLatitude;
						newTreeNode.leftLongitude = treeNode.leftLongitude;
						newTreeNode.rightLongitude = (treeNode.leftLongitude + treeNode.rightLongitude)/2;
						treeNode.setSW(newTreeNode, (splitThreshold+initialScore)/2.0);
					} else { 
						newTreeNode.topLatitude = (treeNode.topLatitude + treeNode.bottomLatitude)/2;
						newTreeNode.bottomLatitude = treeNode.bottomLatitude;
						newTreeNode.leftLongitude = (treeNode.leftLongitude + treeNode.rightLongitude)/2;
						newTreeNode.rightLongitude = treeNode.rightLongitude;
						treeNode.setSE(newTreeNode, (splitThreshold+initialScore)/2.0);
					}
				} else { // North Quadrant
					if (newNode.getLongitude() < (treeNode.leftLongitude + treeNode.rightLongitude)/2) {
						newTreeNode.topLatitude = treeNode.topLatitude;
						newTreeNode.bottomLatitude = (treeNode.topLatitude + treeNode.bottomLatitude)/2;
						newTreeNode.leftLongitude = treeNode.leftLongitude;
						newTreeNode.rightLongitude = (treeNode.leftLongitude + treeNode.rightLongitude)/2;
						treeNode.setNW(newTreeNode, (splitThreshold+initialScore)/2.0);
					} else {
						newTreeNode.topLatitude = treeNode.topLatitude;
						newTreeNode.bottomLatitude = (treeNode.topLatitude + treeNode.bottomLatitude)/2;
						newTreeNode.leftLongitude = (treeNode.leftLongitude + treeNode.rightLongitude)/2;
						newTreeNode.rightLongitude = treeNode.rightLongitude;
						treeNode.setNE(newTreeNode, (splitThreshold+initialScore)/2.0);	
					}
				}
				// remove the node from the global list of available nodes
				availableNodes.remove(newNode);
			}
		} else {
			// the load does not exceed the split threshold OR there is no available node to distribute the load
			treeNode.insertClient(client);
		}
		*/
		treeNode.getInsertNewScore(client);
		
		return treeNode;
	}

	/**
	 * Recursively find a client from the tree and remove it.
	 * 
	 * @param client
	 * @return whether the item is found and has been removed
	 */
	public boolean remove(NodeInfo client) {
		QuadTreeNode treeNode = root;

		do {
			if (treeNode.hasClient(client)) {
				return treeNode.removeClient(client);
			}
			treeNode = treeNode.getQuadrant(client.getLatitude(), client.getLongitude());
		} while (treeNode != null);

		return false;
	}
}
