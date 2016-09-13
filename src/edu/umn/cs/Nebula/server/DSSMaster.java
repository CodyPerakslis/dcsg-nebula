package edu.umn.cs.Nebula.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.gson.Gson;

import edu.umn.cs.Nebula.model.NodeInfo;
import edu.umn.cs.Nebula.model.NodeType;
import edu.umn.cs.Nebula.request.DSSRequest;
import edu.umn.cs.Nebula.request.NodeRequest;
import redis.clients.jedis.Jedis;

public class DSSMaster {
	private static final long maxInactive = 5000; 	// in milliseconds
	private static final int updateInterval = 4000; // in milliseconds
	private static final int nodesPort = 6423;
	private static final int dssPort = 6427;
	private static final int poolSize = 20;
	private static final String jedisServer = "localhost";
	private static final int jedisPort = 6379;
	private static final Jedis jedis = new Jedis(jedisServer, jedisPort);
	private static final Gson gson = new Gson();

	private static HashMap<String, NodeInfo> nodes = new HashMap<String, NodeInfo>();
	private static final Object nodesLock = new Object();

	public static void main(String args[]) {
		// connect to the database
		System.out.print("[DSSMASTER] Initizalizing database connection ");
		jedis.connect();

		// Run a server thread that handles requests from schedulers
		Thread dssServerThread = new Thread(new DSSServerThread());
		dssServerThread.start();

		// Run a server thread that handles requests from nodes
		Thread nodesThread = new Thread(new NodesServerThread());
		nodesThread.start();

		long now;
		NodeInfo nodeInfo = null;
		HashSet<String> removedNodes = new HashSet<String>();
		while (true) {
			now = System.currentTimeMillis();

			synchronized (nodesLock) {
				for (String nodeId: nodes.keySet()) {
					nodeInfo = nodes.get(nodeId);
					// find inactive storage nodes
					if (now - nodeInfo.getLastOnline() > maxInactive) {
						removedNodes.add(nodeId);
					}
				}
				for (String nodeId: removedNodes) {
					nodes.remove(nodeId);
				}
			}
			System.out.println("[DSSMASTER] Storage Nodes: " + nodes.keySet());
			removedNodes.clear();

			try {
				Thread.sleep(updateInterval);
			} catch (InterruptedException e) {
				System.out.println("[DSSMASTER] Main thread interrupted. Thread exits.");
				dssServerThread.interrupt();
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
				System.out.println("[DSSMASTER] Listening for nodes requests on port " + nodesPort);
				while (true) {
					requestPool.submit(new NodesHandler(serverSock.accept()));
				}
			} catch (IOException e) {
				System.err.println("[DSSMASTER] Failed to establish listening socket: " + e);
			} finally {
				requestPool.shutdown();
				if (serverSock != null) {
					try {
						serverSock.close();
					} catch (IOException e) {
						System.err.println("[DSSMASTER] Failed to close listening socket");
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
				System.out.println("[DSSMASTER] Failed parsing node request: " + e.getMessage());
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
				case STORAGE:	// handle get a list of compute nodes
					HashMap<String, NodeInfo> result = new HashMap<String, NodeInfo>();
					result.putAll(nodes);
					out.println(gson.toJson(result));
					break;
				default:
					System.out.println("[DSSMASTER] Invalid node request type: " + nodeRequest.getType());
					out.println(gson.toJson(success));
				}
			} else {
				System.out.println("[DSSMASTER] Request not found or invalid.");
				out.println(gson.toJson(success));
			}
			out.flush();

			try {
				if (in != null) in.close();
				if (out != null) out.close();
				if (clientSock != null) clientSock.close();
			} catch (IOException e) {
				System.out.println("[DSSMASTER] Failed to close streams or socket: " + e.getMessage());
			}
		}
	}


	private static class DSSServerThread implements Runnable {
		@Override
		public void run() {
			// Listening for client requests
			ExecutorService requestPool = Executors.newFixedThreadPool(poolSize);
			ServerSocket serverSock = null;
			try {
				serverSock = new ServerSocket(dssPort);
				System.out.println("[DSSMASTER] Listening for DSS requests on port " + dssPort);
				while (true) {
					requestPool.submit(new DSSHandler(serverSock.accept()));
				}
			} catch (IOException e) {
				System.err.println("[DSSMASTER] Failed to establish listening socket: " + e);
			} finally {
				requestPool.shutdown();
				if (serverSock != null) {
					try {
						serverSock.close();
					} catch (IOException e) {
						System.err.println("[DSSMASTER] Failed to close listening socket");
					}
				}
			}
		}
	}

	private static class DSSHandler implements Runnable {
		private final Socket clientSock;

		public DSSHandler(Socket sock) {
			clientSock = sock;
		}

		@Override
		public void run() {
			BufferedReader in = null;
			PrintWriter out = null;
			DSSRequest dssRequest = null;
			HashMap<String, NodeInfo> result = null;
			boolean success = false;

			try {
				in = new BufferedReader(new InputStreamReader(clientSock.getInputStream()));
				out = new PrintWriter(clientSock.getOutputStream());

				dssRequest = gson.fromJson(in.readLine(), DSSRequest.class);
			} catch (IOException e) {
				System.err.println("[DSSMASTER] Failed parsing DSS request: " + e.getMessage());
				if (out != null) {
					out.println(gson.toJson(false));
					out.flush();
					out.close();
				}
				return;
			}

			if (dssRequest != null) {
				switch (dssRequest.getType()) {
				case GETNODES:
					result = getNodes(0);
					break;
				case GETNODESWITHFILE: // get a list of nodes storing a specific file
					if (dssRequest.getNamespace() != null && dssRequest.getFilename() != null) {
						result = getStorageNodesWithFile(dssRequest.getNamespace(), dssRequest.getFilename());
						out.println(gson.toJson(result));
					} 
					break;
				case NEW: // notification on a new file stored on a specific node
					if (dssRequest.getNamespace() != null && dssRequest.getFilename() != null && dssRequest.getNodeId() != null) {
						success = newFile(dssRequest.getNamespace(), dssRequest.getFilename(), dssRequest.getNodeId());
					}
					out.println(gson.toJson(success)); 
					break;
				case DELETE: // notification on a file has been deleted on a specific node
					if (dssRequest.getNamespace() != null && dssRequest.getFilename() != null && dssRequest.getNodeId() != null) {
						success = deleteFile(dssRequest.getNamespace(), dssRequest.getFilename(), dssRequest.getNodeId());
					}
					out.println(gson.toJson(success)); 
					break;
				default:
					System.out.println("[DSSMASTER] Invalid DSS request: " + dssRequest.getType());
					out.println(gson.toJson(success));
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
				System.err.println("[DSSMASTER] Failed to close streams or socket: " + e.getMessage());
			}
		}
	}



	/**
	 * TODO make it smarter instead of simply give all storage nodes
	 * 
	 * Get a list of storage nodes for data uploading.
	 * @return
	 */
	private static HashMap<String, NodeInfo> getNodes(int option) {
		return nodes;
	}

	/**
	 * Get a list of storage nodes containing a specific file.
	 * 
	 * @param namespace	The namespace
	 * @param filename	The filename
	 * @return 	List of nodes
	 */
	private static HashMap<String, NodeInfo> getStorageNodesWithFile(String namespace, String filename) {		
		String file = namespace + "-" + filename;
		List<String> ids = jedis.lrange(file, 0, -1);
		HashMap<String, NodeInfo> result = new HashMap<String, NodeInfo>();

		synchronized (nodesLock) {
			for (String id: ids) {
				result.put(id, nodes.get(id));
			}
		}

		return result;
	}

	/**
	 * Record a new file that is stored in a specific node.
	 * 
	 * @param namespace	The namespace
	 * @param filename	The name of the file that is uploaded
	 * @param nodeId	The storage node id that stores the file
	 */
	private static boolean newFile(String namespace, String filename, String nodeId) {
		String file = namespace + "-" + filename;

		System.out.println("[DSSMASTER] New File: " + file + " stored in node: " + nodeId);
		jedis.lpush(file, nodeId);
		return true;
	}

	/**
	 * Handle delete request for a specific file on a specific node.
	 * @param namespace	The namespace
	 * @param filename	The filename
	 * @param nodeId	The node that stores the file
	 * @return
	 */
	private static boolean deleteFile(String namespace, String filename, String nodeId) {
		String file = namespace + "-" + filename;
		List<String> nodes = jedis.lrange(file, 0, -1);

		if (nodes == null)
			return false;

		for (String id: nodes) {
			if (id.equals(nodeId)) {
				jedis.lrem(file, 0, nodeId);
				break;
			}
		}
		return true;
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

		if (request != null)
			node = request.getNode();
		if (node == null || node.getNodeType() == null) {
			System.out.println("[DSSMASTER] Invalid node request");
			return success;
		}

		switch (request.getType()) {
		case ONLINE:
			// a request indicating that the node is online/active
			if (!node.getNodeType().equals(NodeType.STORAGE)) {
				return false;
			}
			synchronized (nodesLock) {
				if (nodes.containsKey(node.getId())) {
					nodes.get(node.getId()).updateLastOnline();
					if (node.getBandwidth() > 0) {
						nodes.get(node.getId()).addBandwidth(node.getBandwidth());
					}
				} else {
					nodes.put(node.getId(), node);	
				}
			}
			success = true;
			break;
		case OFFLINE:
			// a request indicating that the node is going to be offline/inactive
			if (!node.getNodeType().equals(NodeType.STORAGE)) {
				return false;
			}
			synchronized (nodesLock) {
				nodes.remove(node.getId());
			}
			success = true;

			break;
		default:
			System.out.println("[DSSMASTER] Invalid request: " + request.getType());
		}
		return success;
	}
}
