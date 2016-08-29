package edu.umn.cs.Nebula.model;

import java.util.ArrayList;
import java.util.HashSet;

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
	public ArrayList<NodeInfo> clients;
	public double topLatitude;
	public double bottomLatitude;
	public double leftLongitude;
	public double rightLongitude;

	public QuadTreeNode(double initScore, NodeInfo node) {
		super(node.getId(), node.getIp(), node.getLatitude(), node.getLongitude(), node.getNodeType());
		parent = NW = NE = SW = SE = null;
		score = initScore;
		clients = new ArrayList<NodeInfo>();
		topLatitude = 90;
		bottomLatitude = -90;
		leftLongitude = -180;
		rightLongitude = 180;
	}

	public QuadTreeNode(double initScore, NodeInfo node, double topLatitude, double bottomLatitude, double leftLongitude, double rightLongitude) {
		super(node.getId(), node.getIp(), node.getLatitude(), node.getLongitude(), node.getNodeType());
		parent = NW = NE = SW = SE = null;
		score = initScore;
		clients = new ArrayList<NodeInfo>();
		this.topLatitude = topLatitude;
		this.bottomLatitude = bottomLatitude;
		this.leftLongitude = leftLongitude;
		this.rightLongitude = rightLongitude;
	}

	public void setParent(QuadTreeNode parent) { this.parent = parent; }
	public QuadTreeNode getParent() { return parent; }
	public QuadTreeNode getNW() { return NW; }
	public QuadTreeNode getNE() { return NE; }
	public QuadTreeNode getSW() { return SW; }
	public QuadTreeNode getSE() { return SE; }

	/**
	 * Add a node to the NW region and transfer all existing nodes in the NW region covered
	 * by the current node into the newly added node.
	 * 
	 * @param nW
	 * @return the new value of the current node
	 */
	public double setNW(QuadTreeNode nW, double maxScore) {
		HashSet<NodeInfo> temp = new HashSet<NodeInfo>();
		if (nW == null) return Double.MIN_VALUE;
		
		NW = nW; 
		for (NodeInfo client: clients) {
			if (client.getLatitude() >= NW.bottomLatitude && client.getLongitude() < NW.rightLongitude) {
				NW.insertClient(client);
				temp.add(client);
				if (score > maxScore) {
					break;
				}
			}
		}
		for (NodeInfo client: temp) {
			removeClient(client);
		}
		return score;
	}

	/**
	 * Add a node to the NE region and transfer all existing nodes in the NW region covered
	 * by the current node into the newly added node.
	 * 
	 * @param nE
	 * @return the new value of the current node
	 */
	public double setNE(QuadTreeNode nE, double maxScore) {
		HashSet<NodeInfo> temp = new HashSet<NodeInfo>();
		if (nE == null) return Double.MIN_VALUE;
		
		NE = nE; 
		for (NodeInfo client: clients) {
			if (client.getLatitude() >= NE.bottomLatitude && client.getLongitude() >= NE.leftLongitude) {
				NE.insertClient(client);
				temp.add(client);
				if (score > maxScore) {
					break;
				}
			}
		}
		for (NodeInfo client: temp) {
			removeClient(client);
		}
		return score;
	}

	/**
	 * Add a node to the SW region and transfer all existing nodes in the NW region covered
	 * by the current node into the newly added node.
	 * 
	 * @param sW
	 * @return the new value of the current node
	 */
	public double setSW(QuadTreeNode sW, double maxScore) { 
		HashSet<NodeInfo> temp = new HashSet<NodeInfo>();
		if (sW == null) return Double.MIN_VALUE;
		
		SW = sW; 
		for (NodeInfo client: clients) {
			if (client.getLatitude() < SW.topLatitude && client.getLongitude() < SW.rightLongitude) {
				SW.insertClient(client);
				temp.add(client);
				if (score > maxScore) {
					break;
				}
			}
		}
		for (NodeInfo client: temp) {
			removeClient(client);
		}
		return score;
	}

	/**
	 * Add a node to the SE region and transfer all existing nodes in the NW region covered
	 * by the current node into the newly added node.
	 * 
	 * @param sE
	 * @return the new value of the current node
	 */
	public double setSE(QuadTreeNode sE, double maxScore) {
		HashSet<NodeInfo> temp = new HashSet<NodeInfo>();
		if (sE == null) return Double.MIN_VALUE;
		
		SE = sE; 
		for (NodeInfo client: clients) {
			if (client.getLatitude() < SE.topLatitude && client.getLongitude() >= SE.leftLongitude) {
				SE.insertClient(client);
				temp.add(client);
				if (score > maxScore) {
					break;
				}
			}
		}
		for (NodeInfo client: temp) {
			removeClient(client);
		}
		return score;
	}

	public double getScore() { return score; }
	public void setScore(double score) { this.score = score; }

	/**
	 * Insert a client into the node and update the score
	 * 
	 * @param client
	 * @return the updated score after insertion
	 */
	public boolean insertClient(NodeInfo client) { 
		score = getInsertNewScore(client);
		return clients.add(client); 
	}
	
	/**
	 * Remove a client from the node and update the score
	 * 
	 * @param client
	 * @return the updated score after removal
	 */
	public boolean removeClient(NodeInfo client) {
		score = getRemoveNewScore(client);
		return clients.remove(client); 
	}
	
	/**
	 * Get the number of clients
	 * 
	 * @return number of clients covered by the node
	 */
	public int getNumClients() { 
		return clients.size(); 
	}
	
	/**
	 * Check if the node covers a specific client
	 * 
	 * @param client
	 * @return has 'client'
	 */
	public boolean hasClient(NodeInfo client) { 
		return clients.contains(client); 
	}

	/**
	 * Get the node that covers the quadrant where the coordinate belongs to
	 * 
	 * @param latitude
	 * @param longitude
	 * @return the node covering the quadrant
	 */
	public QuadTreeNode getQuadrant(double latitude, double longitude) {
		// check if the latitude and longitude value is correct
		if (latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180)
			return null;
		
		// find the corresponding child node
		if (latitude < (topLatitude + bottomLatitude)/2) {	// south quadrant
			if (longitude < (leftLongitude + rightLongitude)/2) return SW; 	// South West Quadrant		
			else return SE; 												// South East Quadrant	
		} else { // north quadrant
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
	
	/**
	 * Get the mean average coordinate of all clients covered by the node
	 * 
	 * @return the mean coordinate of the clients
	 */
	public Coordinate getMeanClientsCoordinate() {
		Coordinate result = new Coordinate(0, 0);
		
		if (clients == null || clients.isEmpty()) {
			return null;
		}
		
		for (NodeInfo client: clients) {
			result.setLatitude(result.getLatitude() + client.getLatitude());
			result.setLongitude(result.getLongitude() + client.getLongitude());
		}
		result.setLatitude(result.getLatitude() / clients.size());
		result.setLongitude(result.getLongitude() / clients.size());
		return result;
	}
}
