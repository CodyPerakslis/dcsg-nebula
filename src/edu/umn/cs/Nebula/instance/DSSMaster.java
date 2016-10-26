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

import com.google.gson.Gson;

import edu.umn.cs.Nebula.model.NodeInfo;
import edu.umn.cs.Nebula.model.NodeType;
import edu.umn.cs.Nebula.request.DSSRequest;
import redis.clients.jedis.Jedis;

public class DSSMaster {
	private static final int dssPort = 6427;
	private static final int poolSize = 20;
	private static final String jedisServer = "localhost";
	private static final int jedisPort = 6379;
	private static final Jedis jedis = new Jedis(jedisServer, jedisPort);
	private static final Gson gson = new Gson();

	private static NodeHandler nodeHandler;

	public static void main(String args[]) {
		if (args.length < 4) {
			nodeHandler = new NodeHandler(6423, NodeType.STORAGE, 5000, 50);
			nodeHandler.connectDB("nebula", "kvm", "localhost", "nebula", 3306); // set to 3306 in hemant, 3307 in local
		} else if (args.length == 4){
			nodeHandler = new NodeHandler(Integer.parseInt(args[0]), NodeType.valueOf(args[1]),
					Integer.parseInt(args[2]), Integer.parseInt(args[3]));
		} else if (args.length == 9) {
			nodeHandler = new NodeHandler(Integer.parseInt(args[0]), NodeType.valueOf(args[1]),
					Integer.parseInt(args[2]), Integer.parseInt(args[3]));
			nodeHandler.connectDB(args[4], args[5], args[6], args[7], Integer.parseInt(args[8]));
		}
		nodeHandler.start();

		System.out.print("[DSSMASTER] Initizalizing Redis connection ");
		jedis.connect();

		// Run a server thread that handles requests from
		start();
	}

	private static void start() {
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
		return nodeHandler.nodes;
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

		synchronized (nodeHandler.nodesLock) {
			for (String id: ids) {
				result.put(id, nodeHandler.nodes.get(id));
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
}
