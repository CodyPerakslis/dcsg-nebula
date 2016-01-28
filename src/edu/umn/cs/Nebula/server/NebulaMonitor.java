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
	private static final long maxInactive = 10000; // 10 seconds
	private static final int updateInterval = 2000; // 5 seconds
	private static final int port = 6422;
	private static final int poolSize = 30;

	private static long now;

	private static HashMap<String, NodeInfo> computeNodes = new HashMap<String, NodeInfo>();
	private static HashMap<String, NodeInfo> storageNodes = new HashMap<String, NodeInfo>();

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
	 * If a node is inactive for >= {@code maxInactive}, the node will be considered offline.
	 * 
	 * @author albert
	 *
	 */
	private static class NodeMonitorThread implements Runnable {

		@Override
		public void run() {
			NodeInfo nodeInfo = null;
			HashSet<String> removedNodes = new HashSet<String>();

			while (true) {
				now = System.currentTimeMillis();

				// find inactive compute nodes
				synchronized (computeNodesLock) {
					for (String nodeId: computeNodes.keySet()) {
						nodeInfo = computeNodes.get(nodeId);

						if (now - nodeInfo.getLastOnline() > maxInactive) {
							removedNodes.add(nodeId);
						}
					}
					for (String nodeId: removedNodes) {
						computeNodes.remove(nodeId);
					}
				}
				removedNodes.clear();

				// find inactive storage nodes
				synchronized (storageNodesLock) {
					for (String nodeId: storageNodes.keySet()) {
						nodeInfo = storageNodes.get(nodeId);

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
			Gson gson = new Gson();

			try {
				in = new BufferedReader(new InputStreamReader (clientSock.getInputStream()));
				out = new PrintWriter(clientSock.getOutputStream(), true);

				// read the input message and parse it as a node request
				String input = in.readLine();
				NodeRequest nodeRequest = gson.fromJson(input, NodeRequest.class);

				if (nodeRequest != null) {
					switch (nodeRequest.getType()) {
					case ONLINE:
					case OFFLINE:
						// handle heartbeat from a node
						NodeInfo node = handleHeartbeat(nodeRequest);
						out.println(gson.toJson(node));
						break;
					case COMPUTE:
						// handle get a list of compute nodes
						result.putAll(computeNodes);
						out.println(gson.toJson(result));
						break;
					case STORAGE:
						// handle get a list of storage nodes
						result.putAll(storageNodes);
						out.println(gson.toJson(result));
						break;
					case ALL:
						// handle get a list of all nodes
						result.putAll(computeNodes);
						result.putAll(storageNodes);
						out.println(gson.toJson(result));
						break;
					default:
						System.out.println("[MONITOR] Request not found. Type: " + nodeRequest.getType());
						out.println(gson.toJson(result));
					}
				} else {
					System.out.println("[MONITOR] Request not found.");
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
		 * Handle a heartbeat request from a node.
		 * 
		 * @param request
		 * @return
		 */
		private NodeInfo handleHeartbeat(NodeRequest request) {
			NodeInfo node = request.getNode();
			NodeInfo result = node;

			if (node == null) {
				System.out.println("[MONITOR] Invalid node.");
				return null;
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
				} else if (node.getNodeType().equals(NodeType.STORAGE)) {
					synchronized (storageNodesLock) {
						if (storageNodes.containsKey(node.getId())) {
							storageNodes.get(node.getId()).updateLastOnline();
						} else {
							storageNodes.put(node.getId(), node);
						}
					}
				}
				break;
			case OFFLINE:
				// a request indicating that the node is going to be offline/inactive
				if (node.getNodeType().equals(NodeType.COMPUTE)) {
					synchronized (computeNodesLock) {
						result = computeNodes.remove(node.getId());
					}
				} else if (node.getNodeType().equals(NodeType.STORAGE)) {
					synchronized (storageNodesLock) {
						result = storageNodes.remove(node.getId());
					}
				}
				break;
			default:
				System.out.println("[MONITOR] Invalid node request type: " + request.getType());
				result = null;
			}
			return result;
		}
	}
}
