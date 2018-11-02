package edu.umn.cs.Nebula.instance;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.gson.Gson;

import edu.umn.cs.Nebula.node.NodeInfo;
import edu.umn.cs.Nebula.request.NodeRequest;
import edu.umn.cs.Nebula.node.NodeType;
import edu.umn.cs.Nebula.util.DatabaseConnector;
import edu.umn.cs.Nebula.util.Grid;


public class NodeManager {
	private final Gson gson = new Gson();
	private final int updateInterval = 3000; // in milliseconds

	private NodeType nodeType;
	private int maxInactive;
	private int poolSize;
	private int port;
	private Thread nodeMonitor;
	private Thread nodeServerThread;

	private DatabaseConnector dbConn;
	private boolean useDatabase = false;

	private LinkedHashMap<String, NodeInfo> nodes;
	private LinkedHashMap<String, Integer> availableResources;
	// private Grid index = new Grid(12, 25, 49, -125, -65);
	private Grid index = new Grid(12, -90, 90, -180, -180);
	public final Object nodesLock = new Object();
	
	private static final boolean DEBUG = true;

	/**
	 * CONSTRUCTOR 
	 * ======================================================================================================== */
	
	public NodeManager(int port, int maxInactive, int poolSize, NodeType nodeType) {
		this.port = port;
		this.maxInactive = maxInactive;
		this.poolSize = poolSize;
		this.nodeType = nodeType;

		nodes = new LinkedHashMap<String, NodeInfo>();
		availableResources = new LinkedHashMap<String, Integer>();
	}

	/** 
	 * UTILITY METHODS
	 * ======================================================================================================== */
	
	public void run() {
		if (DEBUG) System.out.println("[NM] Start nodes monitor");
		nodeMonitor = new Thread(new NodeMonitorThread());
		nodeMonitor.start();

		if (DEBUG) System.out.println("[NM] Start listening for nodes");
		nodeServerThread = new Thread(new NodeServerThread());
		nodeServerThread.start();
	}

	public LinkedHashMap<String, NodeInfo> getNodes() {
		return nodes;
	}
	
	public LinkedHashMap<String, Integer> getAvailableResources() {
		return availableResources;
	}
	
	public int getAvailableResources(String nodeId) {
		return availableResources.get(nodeId);
	}

	public void setAvailableResources(String nodeId, int newValue) {		
		synchronized (nodesLock) {
			if (!availableResources.containsKey(nodeId))
				return;
			availableResources.put(nodeId, newValue);
		}
	}
	
	/**
	 * Connect to the node database. This is used if the handler needs to store the node information to the database.
	 * @param username
	 * @param password
	 * @param serverName
	 * @param dbName
	 * @param dbPort
	 */
	public void connectDB(String username, String password, String serverName, String dbName, int dbPort) {
		// Setup the database connection
		dbConn = new DatabaseConnector(username, password, serverName, dbName, dbPort);
		useDatabase = dbConn.connect();
		if (useDatabase) {
			System.out.println("[NM] Saving node information to " + dbName + " database.");
		} else {
			System.out.println("[NM] Failed connecting to " + dbName + " database.");
		}
	}
	
	/** 
	 * SUBCLASSES
	 * ======================================================================================================== */

	/**
	 * This thread periodically monitors the health of every nodes.
	 * Any node that has been inactive (no heartbeat received) for >= @maxInactive 
	 * will be removed for the list.
	 * 
	 * @author albert
	 */
	private class NodeMonitorThread implements Runnable {
		private long now;
		private NodeInfo nodeInfo = null;
		private String sqlStatement;
		private HashSet<String> removedNodes = new HashSet<String>();

		@Override
		public void run() {
			while (true) {
				now = System.currentTimeMillis();
				synchronized (nodesLock) {
					// find inactive nodes
					for (String nodeId: nodes.keySet()) {
						nodeInfo = nodes.get(nodeId);
						if (now - nodeInfo.getLastOnline() > maxInactive) {
							// remove nodes that have been inactive for more than maxInactive
							removedNodes.add(nodeId);
						}
					}
					for (String nodeId: removedNodes) {
						nodeInfo = nodes.remove(nodeId);
						availableResources.remove(nodeId);
						index.removeItem(nodeId, nodeInfo.getLatitude(), nodeInfo.getLongitude());
						if (useDatabase) {
							sqlStatement = "UPDATE node SET online = " + nodeInfo.getLastOnline()
							+ ", latitude = " + nodeInfo.getLatitude()
							+ ", longitude = " + nodeInfo.getLongitude()
							+ " WHERE id = '" + nodeInfo.getId() + "'"
							+ " AND type = '" + nodeInfo.getNodeType() + "';";
							dbConn.updateQuery(sqlStatement);
						}
					}
				}
				removedNodes.clear();
				if (DEBUG) System.out.println("[NM] Number of active nodes: " + nodes.size());
				try {
					Thread.sleep(updateInterval);
				} catch (InterruptedException e) {
					System.out.println("[NM] Node monitor thread is interrupted and exits: " + e.getMessage());
				}
			}

		}
	}

	/**
	 * This thread waits and accepts connections from nodes at @port 
	 * and launch a handler upon receiving requests. 
	 * 
	 * @author albert
	 */
	private class NodeServerThread implements Runnable {
		@Override
		public void run() {
			// listening for client requests
			ExecutorService requestPool = Executors.newFixedThreadPool(poolSize);
			ServerSocket serverSock = null;
			try {
				serverSock = new ServerSocket(port);
				while (true) {
					requestPool.submit(new NodeRequestHandler(serverSock.accept()));
				}
			} catch (IOException e) {
				System.err.println("[NM] Failed to establish listening socket: " + e);
			} finally {
				requestPool.shutdown();
				if (serverSock != null) {
					try {
						serverSock.close();
					} catch (IOException e) {}
				}
			}
		}
	}

	/** 
	 * HANDLER METHODS
	 * ======================================================================================================== */
	
	/**
	 * Worker thread that handles node requests and heartbeats.
	 * 
	 * @author albert
	 */
	private class NodeRequestHandler implements Runnable {
		private final Socket clientSock;

		public NodeRequestHandler(Socket sock) {
			clientSock = sock;
		}

		@Override
		public void run() {
			BufferedReader in = null;
			PrintWriter out = null;
			NodeRequest nodeRequest;

			try {
				in = new BufferedReader(new InputStreamReader (clientSock.getInputStream()));
				out = new PrintWriter(clientSock.getOutputStream(), true);
				// read the input message and parse it as a node request
				nodeRequest = gson.fromJson(in.readLine(), NodeRequest.class);
			} catch (IOException e) {
				System.out.println("[NM] Failed parsing node request: " + e.getMessage());
				if (out != null) {
					out.println(gson.toJson(false));
					out.flush();
					out.close();
				}
				return;
			}

			boolean success = false;
			LinkedList<String> neighboringNodes;
			if (nodeRequest != null) {
				switch (nodeRequest.getType()) {
				case ONLINE:
				case OFFLINE:	// handle online/offline message from a node
					success = handleHeartbeat(nodeRequest);
					if (success) {
						neighboringNodes = index.getNeighborItems(index.getGridLocation(nodeRequest.getNode().getLatitude(), nodeRequest.getNode().getLongitude()));
						out.println(gson.toJson(neighboringNodes));
					} else {
						out.println(gson.toJson(null));
					}
					break;
				case GET:
					LinkedHashMap<String, NodeInfo> result = new LinkedHashMap<String, NodeInfo>();
					result.putAll(nodes);
					out.println(gson.toJson(result));
					break;
				case GET_NEIGHBORS:
					neighboringNodes = index.getNeighborItems(index.getGridLocation(nodeRequest.getNode().getLatitude(), nodeRequest.getNode().getLongitude()));
					out.println(gson.toJson(neighboringNodes));
					break;
				case GET_NODES:
					neighboringNodes = new LinkedList<String>();
					neighboringNodes.addAll(nodes.keySet());
					out.println(gson.toJson(neighboringNodes));
					break;
				default:
					System.out.println("[NM] Receive an invalid request of type: " + nodeRequest.getType());
					out.print(gson.toJson(success));
				}
			} else {
				out.println(gson.toJson(success));
			}
			out.flush();

			try {
				if (in != null) in.close();
				if (out != null) out.close();
				if (clientSock != null) clientSock.close();
			} catch (IOException e) {
				System.err.println(e);
			}
		}

		/**
		 * Handle an online/offline message from a node.
		 * 
		 * @param request
		 * @return is success?
		 */
		private boolean handleHeartbeat(NodeRequest request) {
			NodeInfo node = null;
			boolean success = false;
			boolean isNewNode = false;
			String sqlStatement;

			if (request != null)
				node = request.getNode();
			if (node == null || node.getNodeType() == null || !node.getNodeType().equals(nodeType)) {
				return false;
			}

			switch (request.getType()) {
			case ONLINE:
				// a request indicating that the node is online/active
				synchronized (nodesLock) {
					if (nodes.containsKey(node.getId())) {
						// we have seen this node before, so simply update its last online
						nodes.get(node.getId()).updateLastOnline();
						if (node.getBandwidth() > 0) {
							nodes.get(node.getId()).addBandwidth(node.getBandwidth());
						}
					} else {
						// new node is online
						isNewNode = true;
						node.updateLastOnline();
						nodes.put(node.getId(), node);
						availableResources.put(node.getId(), node.getResources().getNumCPUs());
						index.insertItem(node.getId(), node.getLatitude(), node.getLongitude());
					}
				}
				success = true;
				
				// save the information about a new node to the DB
				if (useDatabase && isNewNode) {
					sqlStatement = "INSERT INTO node (id, ip, latitude, longitude, type, online)"
							+ " VALUES ('" + node.getId() + "', '" + node.getIp() + "', '" + node.getLatitude()
							+ "', '" + node.getLongitude() + "', '" + node.getNodeType() + "', " + System.currentTimeMillis() + ")"
							+ " ON DUPLICATE KEY UPDATE online="  + System.currentTimeMillis() + ";";
					dbConn.updateQuery(sqlStatement);
				}
				
				break;
			case OFFLINE:
				// a request indicating that the node is going to be offline/inactive
				NodeInfo leavingNode = null;
				
				synchronized (nodesLock) {
					leavingNode = nodes.remove(node.getId());
					availableResources.remove(node.getId());
					index.removeItem(node.getId(), node.getLatitude(), node.getLongitude());
				}
				success = true;
				
				// save the information about a leaving node to the DB
				if (useDatabase && leavingNode != null) {
					sqlStatement = "UPDATE node SET online = " + System.currentTimeMillis()
					+ ", latitude = " + node.getLatitude()
					+ ", longitude = " + node.getLongitude()
					+ " WHERE id = '" + leavingNode.getId() + "'"
					+ " AND type = '" + node.getNodeType() + "';";
					dbConn.updateQuery(sqlStatement);
				}
				
				break;
			default:
				System.out.println("[NM] Invalid request: " + request.getType());
			}
			return success;
		}
	}
}
