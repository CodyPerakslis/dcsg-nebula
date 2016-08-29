package edu.umn.cs.Nebula.server;

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

public class NebulaMonitor {
	private static final long maxInactive = 5000; 	// in milliseconds
	private static final int updateInterval = 4000; // in milliseconds
	private static final int port = 6422;
	private static final int poolSize = 30;

	private static HashMap<String, NodeInfo> computeNodes = new HashMap<String, NodeInfo>();
	private static HashMap<String, NodeInfo> storageNodes = new HashMap<String, NodeInfo>();
	private static Gson gson = new Gson();

	private static final Object computeNodesLock = new Object();
	private static final Object storageNodesLock = new Object();

	// database configuration
	private static String username = "nebula";
	private static String password = "kvm";
	private static String serverName = "localhost";
	private static String dbName = "nebula";
	private static int dbPort = 3306; // set to 3306 in hemant, 3307 in local
	private static DatabaseConnector dbConn;

	public static void main(String args[]) {
		if (args.length < 5) {
			System.out.println("[MONITOR] Using default DB configuration");
		} else {
			username = args[0];
			password = args[1];
			serverName = args[2];
			dbName = args[3];
			dbPort = Integer.parseInt(args[4]);
		}

		// Setup the database connection
		System.out.print("[MONITOR] Connecting to the database ...");	
		dbConn = new DatabaseConnector(username, password, serverName, dbName, dbPort);
		if (!dbConn.connect()) {
			System.out.println("[FAILED]");
			return;
		} else {
			System.out.println("[OK]");
		}

		Thread nodeMonitorThread = new Thread(new NodeMonitorThread());
		nodeMonitorThread.start();

		// Listening for client requests
		ExecutorService requestPool = Executors.newFixedThreadPool(poolSize);
		ServerSocket serverSock = null;
		try {
			serverSock = new ServerSocket(port);
			System.out.println("[MONITOR] Listening for requests on port " + port);
			while (true) {
				requestPool.submit(new MonitorRequestHandler(serverSock.accept()));
			}
		} catch (IOException e) {
			System.err.println("[MONITOR] Failed to establish listening socket: " + e);
		} finally {
			requestPool.shutdown();
			if (serverSock != null) {
				try {
					serverSock.close();
				} catch (IOException e) {
					System.err.println("[MONITOR] Failed to close listening socket");
				}
			}
		}
	}

	/**
	 * This thread class will periodically monitor the status of every node.
	 * If a node has been inactive for > {@code maxInactive}, the node will be considered offline
	 * and removed from the list of online nodes.
	 * 
	 * @author albert
	 */
	private static class NodeMonitorThread implements Runnable {
		@Override
		public void run() {
			long now;
			NodeInfo nodeInfo = null;
			HashSet<String> removedNodes = new HashSet<String>();
			NodeInfo node = null;
			String sqlStatement;

			while (true) {
				now = System.currentTimeMillis();

				synchronized (computeNodesLock) {
					for (String nodeId: computeNodes.keySet()) {
						nodeInfo = computeNodes.get(nodeId);
						// find inactive compute nodes
						if (now - nodeInfo.getLastOnline() > maxInactive) {
							removedNodes.add(nodeId);
						}
					}
					for (String nodeId: removedNodes) {
						node = computeNodes.remove(nodeId);
						sqlStatement = "UPDATE node SET bandwidth = " + node.getBandwidth()
							+ ", online = " + node.getLastOnline()
							+ ", latitude = " + node.getLatitude()
							+ ", longitude = " + node.getLongitude()
							+ " WHERE id = '" + node.getId() + "'"
							+ " AND type = '" + node.getNodeType() + "';";
						dbConn.updateQuery(sqlStatement);
					}
					System.out.println("\n[MONITOR] Compute Nodes: " + computeNodes.keySet());
				}
				removedNodes.clear();

				synchronized (storageNodesLock) {
					for (String nodeId: storageNodes.keySet()) {
						nodeInfo = storageNodes.get(nodeId);
						// find inactive storage nodes
						if (now - nodeInfo.getLastOnline() > maxInactive) {
							removedNodes.add(nodeId);
						}
					}
					for (String nodeId: removedNodes) {
						node = storageNodes.remove(nodeId);
						sqlStatement = "UPDATE node SET bandwidth = " + node.getBandwidth()
							+ ", online = " + node.getLastOnline()
							+ ", latitude = " + node.getLatitude()
							+ ", longitude = " + node.getLongitude()
							+ " WHERE id = '" + node.getId() + "'"
							+ " AND type = '" + node.getNodeType() + "';";
						dbConn.updateQuery(sqlStatement);
					}
					System.out.println("[MONITOR] Storage Nodes: " + storageNodes.keySet());
				}
				removedNodes.clear();
				try {
					Thread.sleep(updateInterval);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * This thread class will handle any request from the node/end-user.
	 * 
	 * @author albert
	 *
	 */
	private static class MonitorRequestHandler implements Runnable {
		private final Socket clientSock;

		public MonitorRequestHandler(Socket sock) {
			clientSock = sock;
		}

		@Override
		public void run() {
			BufferedReader in = null;
			PrintWriter out = null;
			HashMap<String, NodeInfo> result = new HashMap<String, NodeInfo>();

			try {
				in = new BufferedReader(new InputStreamReader (clientSock.getInputStream()));
				out = new PrintWriter(clientSock.getOutputStream(), true);

				// read the input message and parse it as a node request
				String input = in.readLine();
				NodeRequest nodeRequest = gson.fromJson(input, NodeRequest.class);

				if (nodeRequest != null) {
					switch (nodeRequest.getType()) {
					case ONLINE:
					case OFFLINE:	// handle online/offline message from a node
						boolean success = handleHeartbeat(nodeRequest);
						out.println(gson.toJson(success));
						break;
					case COMPUTE:	// handle get a list of compute nodes
						result.putAll(computeNodes);
						out.println(gson.toJson(result));
						break;
					case STORAGE:	// handle get a list of storage nodes
						result.putAll(storageNodes);
						out.println(gson.toJson(result));
						break;
					default:
						System.out.println("[MONITOR] Invalid node request type: " + nodeRequest.getType());
						out.println(gson.toJson(result));
					}
				} else {
					System.out.println("[MONITOR] Request not found or invalid.");
					out.println(gson.toJson(result));
				}
				out.flush();
			} catch (IOException e) {
				System.err.println("Error: " + e);
			} finally {
				try {
					if (in != null) in.close();
					if (out != null) out.close();
					if (clientSock != null) clientSock.close();
				} catch (IOException e) {
					System.out.println("[MONITOR] Failed to close streams or socket: " + e);
				}
			}
		}

		/**
		 * Handle an online/offline message from a node.
		 * 
		 * @param request
		 * @return is success?
		 */
		private boolean handleHeartbeat(NodeRequest request) {
			NodeInfo node = request.getNode();
			boolean success = false;
			String sqlStatement;
			ResultSet queryResult;

			if (node == null || node.getNodeType() == null) {
				System.out.println("[MONITOR] No/invalid node information is found.");
				return success;
			}

			switch (request.getType()) {
			case ONLINE:
				// a request indicating that the node is online/active
				if (node.getNodeType().equals(NodeType.COMPUTE)) {
					synchronized (computeNodesLock) {
						if (computeNodes.containsKey(node.getId())) {
							computeNodes.get(node.getId()).updateLastOnline();
							if (node.getBandwidth() > 0) {
								computeNodes.get(node.getId()).addBandwidth(node.getBandwidth());
							}
						} else {
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
							computeNodes.put(node.getId(), node);
						}
					}
					success = true;
				} else if (node.getNodeType().equals(NodeType.STORAGE)) {
					synchronized (storageNodesLock) {
						if (storageNodes.containsKey(node.getId())) {
							storageNodes.get(node.getId()).updateLastOnline();
							if (node.getBandwidth() > 0) {
								storageNodes.get(node.getId()).addBandwidth(node.getBandwidth());
							}
						} else {
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
							storageNodes.put(node.getId(), node);
						}
					}
					success = true;
				}
				break;
			case OFFLINE:
				NodeInfo savedNode = null;
				// a request indicating that the node is going to be offline/inactive
				if (node.getNodeType().equals(NodeType.COMPUTE)) {
					synchronized (computeNodesLock) {
						savedNode = computeNodes.remove(node.getId());
					}
					success = true;
				} else if (node.getNodeType().equals(NodeType.STORAGE)) {
					synchronized (storageNodesLock) {
						savedNode = storageNodes.remove(node.getId());
					}
					success = true;
				}
				if (savedNode != null) {
					sqlStatement = "UPDATE node SET bandwidth = " + savedNode.getBandwidth()
						+ ", online = " + System.currentTimeMillis()
						+ ", latitude = " + node.getLatitude()
						+ ", longitude = " + node.getLongitude()
						+ " WHERE id = '" + savedNode.getId() + "'"
						+ " AND type = '" + node.getNodeType() + "';";
					dbConn.updateQuery(sqlStatement);
				}
				break;
			default:
				System.out.println("[MONITOR] Invalid node type: " + request.getType());
			}

			return success;
		}
	}
}
