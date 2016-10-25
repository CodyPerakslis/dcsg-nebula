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
import edu.umn.cs.Nebula.model.Lease;
import edu.umn.cs.Nebula.model.NodeInfo;
import edu.umn.cs.Nebula.model.NodeType;
import edu.umn.cs.Nebula.request.NodeRequest;
import edu.umn.cs.Nebula.request.SchedulerRequest;

/**
 * Nebula Resource Manager.
 * 
 * @author albert
 *
 */
public class ResourceManager {
	private static final long maxInactive = 5000; 	// in milliseconds
	private static final int updateInterval = 3000; // in milliseconds
	private static final int nodesPort = 6424;
	private static final int schedulerPort = 6426;
	private static final int poolSize = 50;
	private static final Object nodesLock = new Object();
	private static final Gson gson = new Gson();

	private static HashMap<String, NodeInfo> nodes = new HashMap<String, NodeInfo>();
	private static HashMap<String, Lease> busyNodes = new HashMap<String, Lease>();

	// database configuration
	private static String username = "nebula";
	private static String password = "kvm";
	private static String serverName = "localhost";
	private static String dbName = "nebula";
	private static int dbPort = 3306; // set to 3306 in hemant, 3307 in local
	private static DatabaseConnector dbConn;

	public static void main(String[] args) {
		if (args.length < 5) {
			System.out.println("[RM] Using default DB configuration");
		} else {
			username = args[0];
			password = args[1];
			serverName = args[2];
			dbName = args[3];
			dbPort = Integer.parseInt(args[4]);
		}

		// Setup the database connection
		System.out.print("[RM] Connecting to the database ...");	
		dbConn = new DatabaseConnector(username, password, serverName, dbName, dbPort);
		if (!dbConn.connect()) {
			System.out.println("[FAILED]");
			return;
		} else {
			System.out.println("[OK]");
		}

		// Run a server thread that handles heartbeat from nodes
		Thread nodesThread = new Thread(new NodesServerThread());
		nodesThread.start();

		// Run a server thread that handles requests from schedulers
		Thread schedulerThread = new Thread(new SchedulerServerThread());
		schedulerThread.start();

		long now;
		NodeInfo nodeInfo = null;
		String sqlStatement;
		HashSet<String> removedNodes = new HashSet<String>();
		while (true) {
			now = System.currentTimeMillis();

			synchronized (nodesLock) {
				for (String nodeId: nodes.keySet()) {
					nodeInfo = nodes.get(nodeId);
					// find inactive compute nodes
					if (now - nodeInfo.getLastOnline() > maxInactive) {
						removedNodes.add(nodeId);
					}
				}
			}
			// optionally save inactive nodes information to the DB
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
			System.out.println("[RM] Compute Nodes: " + nodes.keySet());
			removedNodes.clear();

			try {
				Thread.sleep(updateInterval);
			} catch (InterruptedException e) {
				System.out.println("[RM] Main thread interrupted. Thread exits.");
				schedulerThread.interrupt();
				nodesThread.interrupt();
				return;
			}
		}
	}

	private static class NodesServerThread implements Runnable {
		@Override
		public void run() {
			// Listening for client requests
			ExecutorService requestPool = Executors.newFixedThreadPool(poolSize);
			ServerSocket serverSock = null;
			try {
				serverSock = new ServerSocket(nodesPort);
				System.out.println("[RM] Listening for nodes requests on port " + nodesPort);
				while (true) {
					requestPool.submit(new NodesHandler(serverSock.accept()));
				}
			} catch (IOException e) {
				System.err.println("[RM] Failed to establish listening socket: " + e);
			} finally {
				requestPool.shutdown();
				if (serverSock != null) {
					try {
						serverSock.close();
					} catch (IOException e) {
						System.err.println("[RM] Failed to close listening socket");
					}
				}
			}
		}
	}

	private static class NodesHandler implements Runnable {
		private final Socket clientSock;

		public NodesHandler(Socket sock) {
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
				System.out.println("[RM] Failed parsing node request: " + e.getMessage());
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
				case COMPUTE:	// handle get a list of compute nodes
					HashMap<String, NodeInfo> result = new HashMap<String, NodeInfo>();
					result.putAll(nodes);
					out.println(gson.toJson(result));
					break;
				default:
					System.out.println("[RM] Invalid node request type: " + nodeRequest.getType());
					out.println(gson.toJson(success));
				}
			} else {
				System.out.println("[RM] Request not found or invalid.");
				out.println(gson.toJson(success));
			}
			out.flush();

			try {
				if (in != null) in.close();
				if (out != null) out.close();
				if (clientSock != null) clientSock.close();
			} catch (IOException e) {
				System.out.println("[RM] Failed to close streams or socket: " + e.getMessage());
			}
		}
	}

	private static class SchedulerServerThread implements Runnable {
		@Override
		public void run() {
			// Listening for client requests
			ExecutorService requestPool = Executors.newFixedThreadPool(poolSize);
			ServerSocket serverSock = null;
			try {
				serverSock = new ServerSocket(schedulerPort);
				System.out.println("[RM] Listening for scheduler requests on port " + schedulerPort);
				while (true) {
					requestPool.submit(new SchedulerHandler(serverSock.accept()));
				}
			} catch (IOException e) {
				System.err.println("[RM] Failed to establish listening socket: " + e);
			} finally {
				requestPool.shutdown();
				if (serverSock != null) {
					try {
						serverSock.close();
					} catch (IOException e) {
						System.err.println("[RM] Failed to close listening socket");
					}
				}
			}
		}
	}

	/**
	 * Scheduler handler class which communicates with a scheduler for leasing and acquiring nodes.
	 * 
	 * @author albert
	 */
	public static class SchedulerHandler implements Runnable {
		private Socket socket;

		SchedulerHandler(Socket socket) {
			this.socket = socket;
		}

		public void run() {
			PrintWriter out = null;
			BufferedReader in = null;
			SchedulerRequest request;

			try {
				out = new PrintWriter(socket.getOutputStream());
				in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

				request = gson.fromJson(in.readLine(), SchedulerRequest.class);
			} catch (IOException e) {
				System.out.println("[RM] Failed parsing scheduler request: " + e.getMessage());
				if (out != null) {
					out.println(gson.toJson(false));
					out.flush();
					out.close();
				}
				return;
			}

			if (request == null) {
				System.out.println("[RM] Scheduler request not found.");
				out.println(gson.toJson(null));
			} else {
				System.out.println("[RM] Receive a " + request.getType() + " request from " + request.getSchedulerName());

				switch(request.getType()) {
				case GET:
					// return the status of all nodes, including the ones that are busy
					HashMap<String, NodeInfo> reply = new HashMap<String, NodeInfo>();
					NodeInfo nodeInfo;
					synchronized (nodesLock) {
						for (String nodeId: nodes.keySet()) {
							nodeInfo = nodes.get(nodeId);
							// Add the remaining time to expire
							if (busyNodes.containsKey(nodeId)) {
								nodeInfo.setNote("" + busyNodes.get(nodeId).getRemainingTime());
							} else {
								nodeInfo.setNote("available");
							}
							reply.put(nodeId, nodeInfo);
						}
					}
					out.println(gson.toJson(reply));
					break;
				case LEASE:
					HashMap<String, Lease> successfullyLeasedNodes = handleLease(request);
					System.out.println("[RM] Leased: " + successfullyLeasedNodes.keySet() + " by " + request.getSchedulerName());
					out.println(gson.toJson(successfullyLeasedNodes));
					break;
				case RELEASE:
					boolean releaseSuccess = handleRelease(request);
					out.println(gson.toJson(releaseSuccess));
					break;
				default:
					System.out.println("[RM] Invalid request: " + request.getType());
					out.println(gson.toJson(false));
				}
			}
			out.flush();

			try {
				if (in != null) in.close();
				if (out != null) out.close();
				socket.close();
			} catch (IOException e) {
				System.out.println("[RM] Failed closing streams/socket.");
			}
		}
	}

	/**
	 * Handle an online/offline message from a node.
	 * 
	 * @param request
	 * @return is success?
	 */
	private static boolean handleHeartbeat(NodeRequest request) {
		NodeInfo node = null;
		boolean success = false;
		boolean newNode = false;
		String sqlStatement;
		ResultSet queryResult;

		if (request != null)
			node = request.getNode();
		if (node == null || node.getNodeType() == null) {
			System.out.println("[RM] Invalid node request");
			return success;
		}

		switch (request.getType()) {
		case ONLINE:
			// a request indicating that the node is online/active
			if (!node.getNodeType().equals(NodeType.COMPUTE)) {
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
			if (newNode) {
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
			if (!node.getNodeType().equals(NodeType.COMPUTE)) {
				return false;
			}
			synchronized (nodesLock) {
				leavingNode = nodes.remove(node.getId());
			}
			success = true;
			// save the information about a leaving node to the DB
			if (leavingNode != null) {
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
			System.out.println("[RM] Invalid request: " + request.getType());
		}
		return success;
	}

	/**
	 * Lease request from a scheduler on one or more nodes.
	 * 
	 * @param scheduler		the scheduler that sends the leaseRequest
	 * @param leaseRequest 	a map of <nodeId, leaseTime>
	 * @return a set of node id's that are successfully leased
	 */
	private static HashMap<String, Lease> handleLease(SchedulerRequest leaseRequest) {
		HashMap<String, Lease> successfullyLeasedNodes = new HashMap<String, Lease>();

		synchronized (nodesLock) {
			for (String nodeId: leaseRequest.getLeaseNodes()) {
				if (nodes.get(nodeId) == null) {
					// The node is not available
					System.out.println("[RM] " + nodeId + " is not available."); 
					continue;
				}

				if (busyNodes.containsKey(nodeId)) {
					if (busyNodes.get(nodeId).getScheduler() != null && 
							!busyNodes.get(nodeId).getScheduler().equals(leaseRequest.getSchedulerName())) {
						// Do not let a scheduler acquire a node that has already been claimed by another scheduler
						System.out.println("[RM] " + nodeId + " is busy."); 
						continue;
					}
				}

				// TODO add more conditions/constraints such as lease time limit etc.

				// Success. Create a new lease
				busyNodes.put(nodeId, leaseRequest.getLease(nodeId));
				successfullyLeasedNodes.put(nodeId, leaseRequest.getLease(nodeId));
			}
		}
		return successfullyLeasedNodes;
	}

	/**
	 * Free a set of nodes that are leased by a scheduler.
	 * 
	 * @param scheduler
	 * @param nodeIds
	 */
	private static boolean handleRelease(SchedulerRequest leaseRequest) {
		synchronized (nodesLock) {
			for (String nodeId: leaseRequest.getNodes()) {
				if (nodes.get(nodeId) == null || busyNodes.get(nodeId) == null) {
					continue;
				}
				if (busyNodes.get(nodeId).getScheduler() != null && 
						busyNodes.get(nodeId).getScheduler().equals(leaseRequest.getSchedulerName())) {
					// Free the node only if it is currently owned by the scheduler
					busyNodes.remove(nodeId);
				}
			}
		}
		return true;
	}
}
