package edu.umn.cs.Nebula.model;

import java.util.ArrayList;

public class QuadTree {
	private QuadTreeNode root;
	private double splitThreshold;
	private double mergeThreshold;
	private double initialScore;

	public double maxLatitude;
	public double minLatitude;
	public double maxLongitude;
	public double minLongitude;

	public ArrayList<NodeInfo> availableNodes;

	public QuadTree(double initScore, double splitThreshold, double mergeThreshold, NodeInfo root, double maxLatitude, double minLatitude, double maxLongitude, double minLongitude) {
		initialScore = initScore;
		availableNodes = new ArrayList<NodeInfo>();
		this.setSplitThreshold(splitThreshold);
		this.setMergeThreshold(mergeThreshold);

		this.maxLatitude = maxLatitude;
		this.minLatitude = minLatitude;
		this.maxLongitude = maxLongitude;
		this.minLongitude = minLongitude;

		setRoot(new QuadTreeNode(initialScore, root, maxLatitude, minLatitude, minLongitude, maxLongitude));
	}

	public QuadTreeNode getRoot() { return root; }
	public void setRoot(QuadTreeNode root) { this.root = root; }

	public double getSplitThreshold() { return splitThreshold; }
	public double getMergeThreshold() { return mergeThreshold; }
	public void setSplitThreshold(double splitThreshold) { this.splitThreshold = splitThreshold; }
	public void setMergeThreshold(double mergeThreshold) { this.mergeThreshold = mergeThreshold; }

	/**
	 * Get the parent node where the specified item to be inserted into
	 * 
	 * @param latitude
	 * @param longitude
	 * @return
	 */
	public QuadTreeNode getNodeToInsert(double latitude, double longitude) {
		// make sure that the coordinate is valid
		if (latitude < minLatitude || latitude > maxLatitude || longitude < minLongitude || longitude > maxLongitude || root == null)
			return null;

		// find the corresponding node
		QuadTreeNode treeNode = root, temp = root;
		while ((treeNode = treeNode.getQuadrant(latitude, longitude)) != null) {
			// the treeNode has a child node that covers the client's quadrant
			temp = treeNode;
		}
		return temp;
	}

	/**
	 * Add a node into the tree. First find the parent where the node is to be inserted and determine
	 * which region of the parent the node will be inserted into.
	 * 
	 * @param id
	 * @param ip
	 * @param latitude
	 * @param longitude
	 * @param type
	 * @return whether the insertion is success
	 */
	public boolean addNode(String id, String ip, double latitude, double longitude, NodeType type) {
		QuadTreeNode node = new QuadTreeNode(initialScore, new NodeInfo(id, ip, latitude, longitude, type), maxLatitude, minLatitude, minLongitude, maxLongitude);
		QuadTreeNode parent = getNodeToInsert(latitude, longitude);

		if (parent == null) {
			return false;
		}
		node.setParent(parent);

		if (latitude < (parent.topLatitude + parent.bottomLatitude)/2) { // South Quadrant
			if (longitude < (parent.leftLongitude + parent.rightLongitude)/2) {
				node.topLatitude = (parent.topLatitude + parent.bottomLatitude)/2;
				node.bottomLatitude = parent.bottomLatitude;
				node.leftLongitude = parent.leftLongitude;
				node.rightLongitude = (parent.leftLongitude + parent.rightLongitude)/2;
				parent.setSW(node);
			} else { 
				node.topLatitude = (parent.topLatitude + parent.bottomLatitude)/2;
				node.bottomLatitude = parent.bottomLatitude;
				node.leftLongitude = (parent.leftLongitude + parent.rightLongitude)/2;
				node.rightLongitude = parent.rightLongitude;
				parent.setSE(node);
			}
		} else { // North Quadrant
			if (longitude < (parent.leftLongitude + parent.rightLongitude)/2) {
				node.topLatitude = parent.topLatitude;
				node.bottomLatitude = (parent.topLatitude + parent.bottomLatitude)/2;
				node.leftLongitude = parent.leftLongitude;
				node.rightLongitude = (parent.leftLongitude + parent.rightLongitude)/2;
				parent.setNW(node);
			} else {
				node.topLatitude = parent.topLatitude;
				node.bottomLatitude = (parent.topLatitude + parent.bottomLatitude)/2;
				node.leftLongitude = (parent.leftLongitude + parent.rightLongitude)/2;
				node.rightLongitude = parent.rightLongitude;
				parent.setNE(node);	
			}
		}
		return true;
	}

	public QuadTreeNode removeNode(String id, double latitude, double longitude) {
		// make sure that the coordinate is valid
		if (latitude < minLatitude || latitude > maxLatitude || longitude < minLongitude || longitude > maxLongitude || root == null)
			return null;

		// root node
		if (root.getId().equals(id)) {
			QuadTreeNode temp = root;
			root = null;
			return temp;	
		}

		// find the corresponding node
		QuadTreeNode treeNode = root, temp = null;
		while ((treeNode = treeNode.getQuadrant(latitude, longitude)) != null) {
			// the treeNode has a child node that covers the client's quadrant
			if (treeNode.getId().equals(id)) {
				// the node to be removed is found
				temp = treeNode;

				// if it's a leaf node
				if (treeNode.isLeaf()) {
					if (temp.getParent().getNW() != null && temp.getParent().getNW().getId().equals(temp.getId())) {
						treeNode.getParent().setNW(null);
					} else if (temp.getParent().getNE() != null && temp.getParent().getNE().getId().equals(temp.getId())) {
						treeNode.getParent().setNE(null);
					} else if (temp.getParent().getSW() != null && temp.getParent().getSW().getId().equals(temp.getId())) {
						treeNode.getParent().setSW(null);
					} else if (temp.getParent().getSE() != null && temp.getParent().getSE().getId().equals(temp.getId())) {
						treeNode.getParent().setSE(null);
					}
					break;
				}

				// non-leaf node
				while (!treeNode.isLeaf()) {
					if (treeNode.getNW() != null) {
						treeNode = treeNode.getNW();
					} else if (treeNode.getNE() != null) {
						treeNode = treeNode.getNE();
					} else if (treeNode.getSW() != null) {
						treeNode = treeNode.getSW();
					} else {
						treeNode = treeNode.getSE();
					}
				}

				treeNode.setParent(temp.getParent());
				if (temp.getNW() != null && !temp.getNW().getId().equals(treeNode.getId()))
					treeNode.setNW(temp.getNW());
				if (temp.getNE() != null && !temp.getNE().getId().equals(treeNode.getId()))
					treeNode.setNE(temp.getNE());
				if (temp.getSW() != null && !temp.getSW().getId().equals(treeNode.getId()))
					treeNode.setSW(temp.getSW());
				if (temp.getSE() != null && !temp.getSE().getId().equals(treeNode.getId()))
					treeNode.setSE(temp.getSE());

				treeNode.topLatitude = temp.topLatitude;
				treeNode.bottomLatitude = temp.bottomLatitude;
				treeNode.leftLongitude = temp.leftLongitude;
				treeNode.rightLongitude = temp.rightLongitude;

				if (temp.getParent().getNW() != null && temp.getParent().getNW().getId().equals(temp.getId())) {
					treeNode.getParent().setNW(treeNode);
				} else if (temp.getParent().getNE() != null && temp.getParent().getNE().getId().equals(temp.getId())) {
					treeNode.getParent().setNE(treeNode);
				} else if (temp.getParent().getSW() != null && temp.getParent().getSW().getId().equals(temp.getId())) {
					treeNode.getParent().setSW(treeNode);
				} else if (temp.getParent().getSE() != null && temp.getParent().getSE().getId().equals(temp.getId())) {
					treeNode.getParent().setSE(treeNode);
				}
			}
		}

		return temp;
	}

	/**
	 * Insert a client into the tree starting from the root.
	 * 
	 * @param 	client		the client to be inserted
	 * @return	the node to which the client is inserted into, null if failed
	 */
	public NodeInfo insertItem(NodeInfo client) {
		// make sure that the client's coordinate is valid
		if (client.getLatitude() < minLatitude || client.getLatitude() > maxLatitude || client.getLongitude() < minLongitude || client.getLongitude() > maxLongitude)
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
