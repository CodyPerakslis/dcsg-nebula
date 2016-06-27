package edu.umn.cs.Nebula.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.gson.Gson;

import edu.umn.cs.Nebula.node.NodeInfo;
import edu.umn.cs.Nebula.node.NodeType;
import edu.umn.cs.Nebula.request.NodeRequest;

public class NebulaMonitor {
	private static final long maxInactive = 5000; 	// in milliseconds
	private static final int updateInterval = 2000; // in milliseconds
	private static final int port = 6422;
	private static final int poolSize = 30;

	private static HashMap<String, NodeInfo> computeNodes = new HashMap<String, NodeInfo>();
	private static HashMap<String, NodeInfo> storageNodes = new HashMap<String, NodeInfo>();
	private static Gson gson = new Gson();

	private static final Object computeNodesLock = new Object();
	private static final Object storageNodesLock = new Object();

	public static void main(String args[]) {
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
						computeNodes.remove(nodeId);
					}
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
						storageNodes.remove(nodeId);
					}
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
					System.out.println("Failed to close streams or socket: " + e);
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
						} else {
							computeNodes.put(node.getId(), node);
						}
					}
					success = true;
				} else if (node.getNodeType().equals(NodeType.STORAGE)) {
					synchronized (storageNodesLock) {
						if (storageNodes.containsKey(node.getId())) {
							storageNodes.get(node.getId()).updateLastOnline();
						} else {
							storageNodes.put(node.getId(), node);
						}
					}
					success = true;
				}
				break;
			case OFFLINE:
				// a request indicating that the node is going to be offline/inactive
				if (node.getNodeType().equals(NodeType.COMPUTE)) {
					synchronized (computeNodesLock) {
						computeNodes.remove(node.getId());
					}
					success = true;
				} else if (node.getNodeType().equals(NodeType.STORAGE)) {
					synchronized (storageNodesLock) {
						storageNodes.remove(node.getId());
					}
					success = true;
				}
				break;
			default:
				System.out.println("[MONITOR] Invalid node type: " + request.getType());
			}
			return success;
		}
	}
}
