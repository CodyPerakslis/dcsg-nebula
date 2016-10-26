package edu.umn.cs.Nebula.instance;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.gson.Gson;

import edu.umn.cs.Nebula.model.DatabaseConnector;
import edu.umn.cs.Nebula.model.NodeInfo;
import edu.umn.cs.Nebula.model.NodeType;
import edu.umn.cs.Nebula.request.NodeRequest;

public class NodeHandler {
	private final Gson gson = new Gson();
	private final int updateInterval = 3000; // in milliseconds
	
	private int maxInactive;
	private int poolSize;
	private int port;
	private NodeType nodeType;
	private DatabaseConnector dbConn;
	private boolean databaseConnected = false;
	private Thread nodeMonitor;
	private Thread nodeServerThread;
	
	public HashMap<String, NodeInfo> nodes = new HashMap<String, NodeInfo>();
	public final Object nodesLock = new Object();

	public NodeHandler(int port, NodeType nodeType, int maxInactive, int poolSize) {
		this.port = port;
		this.nodeType = nodeType;
		this.maxInactive = maxInactive;
		this.poolSize = poolSize;
	}

	public void start() {
		nodeMonitor = new Thread(new NodeMonitorThread());
		nodeMonitor.start();
		
		nodeServerThread = new Thread(new NodeServerThread());
		nodeServerThread.start();
	}
	
	/**
	 * Connect to the node database. This is used if the handler needs to store the node information to the database.
	 * @param username
	 * @param password
	 * @param serverName
	 * @param dbName
	 * @param dbPort
	 * @return
	 */
	public boolean connectDB(String username, String password, String serverName, String dbName, int dbPort) {
		// Setup the database connection
		dbConn = new DatabaseConnector(username, password, serverName, dbName, dbPort);
		databaseConnected = dbConn.connect();
		return databaseConnected;
	}

	/**
	 * This thread will periodically monitor the nodes that are registered/monitored.
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
					// find inactive compute nodes
					for (String nodeId: nodes.keySet()) {
						nodeInfo = nodes.get(nodeId);
						if (now - nodeInfo.getLastOnline() > maxInactive) {
							// remove nodes that have been inactive for more than maxInactive
							removedNodes.add(nodeId);
						}
					}
				}
				// optionally save inactive nodes information to the DB
				if (databaseConnected) {
					for (String nodeId: removedNodes) {
						nodeInfo = nodes.remove(nodeId);
						sqlStatement = "UPDATE node SET bandwidth = " + nodeInfo.getBandwidth()
							+ ", online = " + nodeInfo.getLastOnline()
							+ ", latitude = " + nodeInfo.getLatitude()
							+ ", longitude = " + nodeInfo.getLongitude()
							+ " WHERE id = '" + nodeInfo.getId() + "'"
							+ " AND type = '" + nodeInfo.getNodeType() + "';";
						dbConn.updateQuery(sqlStatement);
					}
				}
				removedNodes.clear();
				System.out.println("[NODE_HANDLER] " + nodeType + " nodes: " + nodes.keySet());
				try {
					Thread.sleep(updateInterval);
				} catch (InterruptedException e) {
					System.out.println("[NODE_HANDLER] Node monitor thread interrupted and exits: " + e.getMessage());
				}
			}

		}
	}
	
	/**
	 * This thread will wait and accept connections from nodes at port @port 
	 * and launch a handler upon receiving requests. 
	 * 
	 * @author albert
	 */
	private class NodeServerThread implements Runnable {
		@Override
		public void run() {
			// Listening for client requests
			ExecutorService requestPool = Executors.newFixedThreadPool(poolSize);
			ServerSocket serverSock = null;
			try {
				serverSock = new ServerSocket(port);
				while (true) {
					requestPool.submit(new NodeRequestHandler(serverSock.accept()));
				}
			} catch (IOException e) {
				System.err.println("[NODE_HANDLER] Failed to establish listening socket: " + e);
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
	 * Worker thread that handles node requests and heartbeat.
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
				System.out.println("[NODE_HANDLER] Failed parsing node request: " + e.getMessage());
				if (out != null) {
					out.println(gson.toJson(false));
					out.flush();
					out.close();
				}
				return;
			}

			boolean success = false;
			if (nodeRequest != null) {
				switch (nodeRequest.getType()) {
				case ONLINE:
				case OFFLINE:	// handle online/offline message from a node
					success = handleHeartbeat(nodeRequest);
					out.println(gson.toJson(success));
					break;
				default:
					if (nodeRequest.getType().equals(nodeType)) {
						HashMap<String, NodeInfo> result = new HashMap<String, NodeInfo>();
						result.putAll(nodes);
						out.println(gson.toJson(result));
					} else {
						out.println(gson.toJson(success));
					}
				}
			} else {
				out.println(gson.toJson(success));
			}
			out.flush();

			try {
				if (in != null) in.close();
				if (out != null) out.close();
				if (clientSock != null) clientSock.close();
			} catch (IOException e) {}
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
			boolean newNode = false;
			String sqlStatement;
			ResultSet queryResult;

			if (request != null)
				node = request.getNode();
			if (node == null || node.getNodeType() == null) {
				return success;
			}

			switch (request.getType()) {
			case ONLINE:
				// a request indicating that the node is online/active
				if (!node.getNodeType().equals(nodeType)) {
					return false;
				}
				synchronized (nodesLock) {
					if (nodes.containsKey(node.getId())) {
						nodes.get(node.getId()).updateLastOnline();
						if (node.getBandwidth() > 0) {
							nodes.get(node.getId()).addBandwidth(node.getBandwidth());
						}
					} else {
						newNode = true;
						nodes.put(node.getId(), node);
					}
				}
				success = true;

				// save the information about a new node to the DB
				if (databaseConnected && newNode) {
					sqlStatement = "SELECT bandwidth, latency FROM node"
							+ " WHERE id = '" + node.getId() + "' "
							+ " AND type = '" + node.getNodeType() + "';";
					queryResult = dbConn.selectQuery(sqlStatement);
					try {
						if (queryResult != null && queryResult.next()) {
							node.addBandwidth(queryResult.getDouble("bandwidth"));
							node.addLatency(queryResult.getDouble("latency"));
						} else {
							sqlStatement = "INSERT INTO node (id, ip, latitude, longitude, bandwidth, latency, type, online)"
									+ " VALUES ('" + node.getId() + "', '" + node.getIp() + "', '" + node.getLatitude()
									+ "', '" + node.getLongitude() + "', " + node.getBandwidth() + ", " + node.getLatency()
									+ ", '" + node.getNodeType() + "', '" + System.currentTimeMillis() + "');";
							dbConn.updateQuery(sqlStatement);
						}
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
				break;
			case OFFLINE:
				NodeInfo leavingNode = null;
				// a request indicating that the node is going to be offline/inactive
				if (!node.getNodeType().equals(nodeType)) {
					return false;
				}
				synchronized (nodesLock) {
					leavingNode = nodes.remove(node.getId());
				}
				success = true;
				// save the information about a leaving node to the DB
				if (databaseConnected && leavingNode != null) {
					sqlStatement = "UPDATE node SET bandwidth = " + leavingNode.getBandwidth()
						+ ", online = " + System.currentTimeMillis()
						+ ", latitude = " + node.getLatitude()
						+ ", longitude = " + node.getLongitude()
						+ " WHERE id = '" + leavingNode.getId() + "'"
						+ " AND type = '" + node.getNodeType() + "';";
						dbConn.updateQuery(sqlStatement);
				}
				break;
			default:
				System.out.println("[NODE_HANDLER] Invalid request: " + request.getType());
			}
			return success;
		}
	}
}
