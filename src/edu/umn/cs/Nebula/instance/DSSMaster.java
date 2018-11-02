package edu.umn.cs.Nebula.instance;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import redis.clients.jedis.Jedis;

import com.google.gson.Gson;

import edu.umn.cs.Nebula.node.NodeInfo;
import edu.umn.cs.Nebula.node.NodeType;
import edu.umn.cs.Nebula.request.DSSRequest;

public class DSSMaster {
	private static final int dssPort = 6427;
	private static final int poolSize = 20;
	
	private static final String jedisServer = "localhost";
	private static final int jedisPort = 6379;
	private static final Jedis jedis = new Jedis(jedisServer, jedisPort);
	private static final Gson gson = new Gson();

	private static NodeManager nodeManager;
	
	/** 
	 * INITIALIZATION
	 * ======================================================================================================== */

	public static void main(String args[]) {
		int nodeManagerPort = 6423;
		int nodeManagerMaxInactive = 5000;
		int nodeManagerPoolSize = 50;
		String nodeDatabaseUsername = "nebula";
		String nodeDatabasePassword = "kvm";
		String nodeDatabaseServerName = "localhost";
		String nodeDatabaseName = "nebula";
		int nodeDatabasePort = 3306; // 3306 in hemant, 3307 in local

		if (args.length == 3) {
			nodeManagerPort = Integer.parseInt(args[0]);
			nodeManagerMaxInactive = Integer.parseInt(args[1]);
			nodeManagerPoolSize = Integer.parseInt(args[2]);
		} else if (args.length == 8) {
			nodeManagerPort = Integer.parseInt(args[0]);
			nodeManagerMaxInactive = Integer.parseInt(args[1]);
			nodeManagerPoolSize = Integer.parseInt(args[2]);
			nodeDatabaseUsername = args[3];
			nodeDatabasePassword = args[4];
			nodeDatabaseServerName = args[5];
			nodeDatabaseName = args[6];
			nodeDatabasePort = Integer.parseInt(args[7]);
		}

		nodeManager = new NodeManager(nodeManagerPort, nodeManagerMaxInactive, nodeManagerPoolSize, NodeType.STORAGE);
		nodeManager.connectDB(nodeDatabaseUsername, nodeDatabasePassword, nodeDatabaseServerName, nodeDatabaseName,
				nodeDatabasePort);
		nodeManager.run();

		// connect to redis
		System.out.print("[DSSMASTER] Initizalizing Redis connection ");
		jedis.connect();

		start();
	}

	/**
	 * Start listening for DSS requests (requests related to file management)
	 */
	private static void start() {
		// listening for client requests
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
	
	/** 
	 * SUBCLASSES
	 * ======================================================================================================== */

	private static class DSSHandler implements Runnable {
		private final Socket clientSocket;

		public DSSHandler(Socket socket) {
			clientSocket = socket;
		}

		@Override
		public void run() {
			BufferedReader in = null;
			PrintWriter out = null;
			DSSRequest dssRequest = null;
			HashMap<String, NodeInfo> result = null;
			boolean success = false;

			try {
				in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
				out = new PrintWriter(clientSocket.getOutputStream());

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
					result = nodeManager.getNodes();
					break;
				case GETNODESWITHFILE: // get a list of nodes storing the file
					if (dssRequest.getNamespace() != null && dssRequest.getFilename() != null) {
						result = getStorageNodesWithFile(dssRequest.getNamespace(), dssRequest.getFilename());
						out.println(gson.toJson(result));
					}
					break;
				case NEW: // notification indicating a new file is stored on the node
					if (dssRequest.getNamespace() != null && dssRequest.getFilename() != null && dssRequest.getNodeId() != null) {
						success = newFile(dssRequest.getNamespace(), dssRequest.getFilename(), dssRequest.getNodeId());
					}
					out.println(gson.toJson(success));
					break;
				case DELETE: // notification indicating the file has been removed from the node
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
				if (clientSocket != null) clientSocket.close();
			} catch (IOException e) {
				System.err.println("[DSSMASTER] Failed to close streams or socket: " + e.getMessage());
			}
		}
	}

	/** 
	 * HANDLER METHODS
	 * ======================================================================================================== */
	
	/**
	 * Get a list of storage nodes containing a specific file.
	 * 
	 * @param namespace
	 * @param filename
	 * @return List of nodes
	 */
	private static HashMap<String, NodeInfo> getStorageNodesWithFile(String namespace, String filename) {
		String fileId = namespace + "-" + filename;
		List<String> nodeIds = jedis.lrange(fileId, 0, -1);
		HashMap<String, NodeInfo> result = new HashMap<String, NodeInfo>();

		synchronized (nodeManager.nodesLock) {
			for (String nodeId : nodeIds) {
				result.put(nodeId, nodeManager.getNodes().get(nodeId));
			}
		}
		return result;
	}

	/**
	 * Record a new file that is stored in a specific node.
	 * 
	 * @param namespace
	 * @param filename
	 * @param nodeId
	 */
	private static boolean newFile(String namespace, String filename, String nodeId) {
		String fileId = namespace + "-" + filename;

		System.out.println("[DSSMASTER] A new file " + fileId + " is stored in node " + nodeId);
		jedis.lpush(fileId, nodeId);
		return true;
	}

	/**
	 * Handle delete request for a specific file on a specific node.
	 * 
	 * @param namespace
	 * @param filename
	 * @param nodeId
	 * @return
	 */
	private static boolean deleteFile(String namespace, String filename, String node) {
		String fileId = namespace + "-" + filename;
		List<String> nodeIds = jedis.lrange(fileId, 0, -1);

		if (nodeIds == null) return false;

		for (String nodeId : nodeIds) {
			if (nodeId.equals(node)) {
				jedis.lrem(fileId, 0, nodeId);
				break;
			}
		}
		return true;
	}
}
