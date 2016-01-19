package edu.umn.cs.MCC.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.gson.Gson;

import edu.umn.cs.MCC.model.Location;
import edu.umn.cs.MCC.model.MobileRequest;
import edu.umn.cs.MCC.model.Node;
import edu.umn.cs.MCC.model.NodeRequest;
import edu.umn.cs.MCC.model.NodeType;

public class CloudServer {

	private static final int port = 6420;
	private static final int poolSize = 100;
	private static final int gridSize = 10;
	private static final long maxInactive = 20000;

	private static Object nodesLock = new Object();
	private static HashMap<String, Node> primaryNodes = new HashMap<String, Node>();
	private static HashMap<Integer, HashMap<Integer, HashMap<String, Double>>> locationNodesMap = new HashMap<>();
	// <x, <y, <id, score>>>

	/**
	 * Get a primary node for a mobileUser. 
	 * The implementation of this method may be different depending on the application's needs.
	 * 
	 * @param mobileUser
	 * @return
	 */
	public static String getNode(Node mobileUser) {
		String nodeId = null;
		Double bestScore = -1.0;
		Location loc = new Location(gridSize, mobileUser.getLatitude(), mobileUser.getLongitude());
		int x = loc.getxPosition();
		int y = loc.getyPosition();

		// get a primary node based on the user's location.
		// find a node that lies within the same grid location and has the best score.
		if (locationNodesMap.containsKey(x) && locationNodesMap.get(x).containsKey(y)) {
			for (Entry<String, Double> node: locationNodesMap.get(x).get(y).entrySet()) {
				// find a node with the lowest value (best score)
				if (bestScore < 0 || bestScore > node.getValue()) {
					nodeId = node.getKey();
					bestScore = node.getValue();
				}
			}
			// update the node's score
			if (nodeId == null)
				return null;

			if (bestScore == Double.MAX_VALUE) {
				// this only happens when all of the nodes have the maximum score, just reset the scores back to 0.0
				for (Entry<String, Double> node: locationNodesMap.get(x).get(y).entrySet()) {
					locationNodesMap.get(x).get(y).put(node.getKey(), 0.0);
				}
			}

			// update the score of the node
			bestScore += 1;
			locationNodesMap.get(x).get(y).put(nodeId, bestScore);
		} else {
			// TODO find another node on different location?
			System.out.println("No node found in location: (" + x + "," + y + ")");
		}

		return nodeId;
	}

	public static void main(String[] args) {
		// Listening for client requests
		Thread nodeMonitorThread = new Thread(new NodeMonitorThread());
		nodeMonitorThread.start();
		
		ExecutorService requestPool = Executors.newFixedThreadPool(poolSize);
		ServerSocket serverSock = null;
		try {
			serverSock = new ServerSocket(port);
			System.out.println("Listening for requests on port " + port);
			while (true) {
				requestPool.submit(new RequestThread(serverSock.accept()));
			}
		} catch (IOException e) {
			System.err.println("Failed to establish listening socket: " + e);
		} finally {
			requestPool.shutdown();
			if (serverSock != null) {
				try {
					serverSock.close();
				} catch (IOException e) {
					System.err.println("Failed to close listening socket");
				}
			}
		}
	}
	
	/**
	 * This thread will periodically monitor the status of each primary nodes.
	 * If it does not receive a heartbeat for a predetermined time limit, {@code maxInactive},
	 * the node will be considered inactive and removed from the list of active nodes.
	 * 
	 * @author albert
	 *
	 */
	private static class NodeMonitorThread implements Runnable {
		private int updateInterval = 10000; // 10 seconds
		private Date now = new Date();
		private Set<String> removedNodes = new HashSet<String>();

		@Override
		public void run() {
			while (true) {
				now = new Date();
				synchronized (nodesLock) {
					for (String nodeId: primaryNodes.keySet()) {
						if (now.getTime() - primaryNodes.get(nodeId).getLastOnline().getTime() > maxInactive) {
							removedNodes.add(nodeId);
						}
					}
					for (String nodeId: removedNodes) {
						primaryNodes.remove(nodeId);
					}
					removedNodes.clear();
					System.out.println("Online nodes: " + primaryNodes.keySet());
				}
				
				try {
					Thread.sleep(updateInterval);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
	}

	/**
	 * This thread handles any request from either primary nodes or mobile users.
	 * 
	 * @author albert
	 *
	 */
	private static class RequestThread implements Runnable {
		private final Socket clientSock;

		public RequestThread(Socket sock) {
			clientSock = sock;
		}
		
		@Override
		public void run() {
			BufferedReader in = null;
			PrintWriter out = null;
			String response = "failed";
			Gson gson = new Gson();

			try {
				in = new BufferedReader(new InputStreamReader (clientSock.getInputStream()));
				out = new PrintWriter(clientSock.getOutputStream(), true);

				String input = in.readLine();
				if (input.equalsIgnoreCase("Nodes")) {
					// return a list of online primary nodes
					out.println(gson.toJson(primaryNodes));
					out.flush();
					return;
				}
				
				NodeRequest nodeRequest = gson.fromJson(input, NodeRequest.class);
				MobileRequest mobileRequest = gson.fromJson(input, MobileRequest.class);
								
				if (nodeRequest != null && nodeRequest.getNode() != null) {
					// handle a primary node's request
					HashMap<String, Node> nodes = handleNodeRequest(nodeRequest);
					out.println(gson.toJson(nodes));
				} else if (mobileRequest != null) {
					// handle a mobile user's request
					response = handleMobileRequest(mobileRequest);
					out.println(gson.toJson(response));
				} else {
					// unsupported request, return a list of online primary nodes
					out.println(gson.toJson(primaryNodes));
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
					System.out.println("Failed to close streams or socket: " + e);
				}
			}
		}

		/**
		 * Handle a request from primary node. The request's type is defined in {@code NodeRequestType.java}
		 * 
		 * @param request
		 * @return
		 */
		@SuppressWarnings("unchecked")
		private HashMap<String, Node> handleNodeRequest(NodeRequest request) {
			Node node = request.getNode();
			HashMap<String, Node> result = null;
			double initScore = 0.0;
			
			if (node == null) {
				System.out.println("Invalid node.");
				return null;
			}
			
			switch (request.getType()) {
			case ONLINE:
				// a request indicating that the node is online/active
				synchronized (nodesLock) {
					if (primaryNodes.containsKey(node.getId())) {
						primaryNodes.get(node.getId()).updateLastOnline();
					} else {
						primaryNodes.put(node.getId(), node);
					}
					
					// get the grid location of the node
					Location loc = new Location(gridSize, node.getLatitude(), node.getLongitude());
					int x = loc.getxPosition();
					int y = loc.getyPosition();
					
					HashMap<String, Double> temp;
					HashMap<Integer, HashMap<String, Double>> temp1;
					
					if (locationNodesMap.containsKey(x)) {
						temp1 = locationNodesMap.get(x);
						if (locationNodesMap.get(x).containsKey(y)) {
							temp = locationNodesMap.get(x).get(y);
						} else {
							temp = new HashMap<String, Double>();
						}
					} else {
						temp1 = new HashMap<Integer, HashMap<String, Double>>();
						temp = new HashMap<String, Double>();
					}
					// initialize the score of the node 
					temp.put(node.getId(), initScore);
					temp1.put(y, temp);
					locationNodesMap.put(x, temp1);
					result = (HashMap<String, Node>) primaryNodes.clone();
				}
				// give a list of all primary nodes, excluding the node itself
				result.remove(node.getId());
				break;
			case OFFLINE:
				// a request indicating that the node is going to be offline/inactive
				synchronized (nodesLock) {
					primaryNodes.remove(node.getId());
					Location loc = new Location(gridSize, node.getLatitude(), node.getLongitude());
					int x = loc.getxPosition();
					int y = loc.getyPosition();
					
					if (locationNodesMap.containsKey(x) && locationNodesMap.get(x).containsKey(y) &&
							locationNodesMap.get(x).get(y).containsKey(node.getId())) {
						locationNodesMap.get(x).get(y).remove(node.getId());
					}
				}
				result = (HashMap<String, Node>) primaryNodes.clone();
				break;
			default:
				System.out.println("Invalid node request type: " + request.getType());
			}
			return result;
		}

		/**
		 * Handle a request from mobile application. The request's type is defined in {@code MobileRequestType.java}
		 * 
		 * @param request
		 * @return
		 */
		private String handleMobileRequest(MobileRequest request) {
			String status = "failed";
			
			switch (request.getType()) {
			case LOCATION:
				// a request for getting a primary node to support the mobile user
				Node mobileUser = new Node(request.getUserId(), 
						request.getUserIp(), 
						request.getLatitude(), request.getLongitude(), NodeType.MOBILE);
				// get a node for the mobile user
				status = getNode(mobileUser);
				break;
			default:
				System.out.println("Undefined request type: " + request.getType());
			}
			return status;
		}
	}
}
