package edu.umn.cs.Nebula.server;

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
import edu.umn.cs.Nebula.request.DSSRequest;
import edu.umn.cs.Nebula.request.NodeRequest;
import redis.clients.jedis.Jedis;

public class NebulaDSSMaster {

	private static final int port = 6423;
	private static final int poolSize = 10;

	private static Jedis jedis;
	private static final String jedisServer = "localhost";
	private static final int jedisPort = 6379;
	private static final Gson gson = new Gson();

	private static HashMap<String, NodeInfo> storageNodes;
	private static final Object nodesLock = new Object();

	public static void main(String args[]) {
		// connect to the database
		System.out.print("[DSSMaster] Initizalizing database connection ... ");
		storageNodes = new HashMap<String, NodeInfo>();
		jedis = new Jedis(jedisServer, jedisPort);
		jedis.connect();
		System.out.println("[OK]");

		// listen to DSS requests
		ExecutorService requestPool = Executors.newFixedThreadPool(poolSize);
		ServerSocket serverSock = null;
		try {
			serverSock = new ServerSocket(port);
			System.out.println("[DSSMaster] Listening for DSS requests on port " + port);
			while (true) {
				requestPool.submit(new DSSThread(serverSock.accept()));
			}
		} catch (IOException e) {
			System.err.println("[DSSMaster] Failed to establish listening socket: " + e);
		} finally {
			requestPool.shutdown();
			if (serverSock != null) {
				try {
					serverSock.close();
				} catch (IOException e) {
					System.err.println("[DSSMaster] Failed to close listening socket");
				}
			}
		}
	}

	/**
	 * This thread class will periodically update the list of online storage nodes
	 * from Nebula Monitor.
	 * @author albert
	 *
	private static class MonitorThread implements Runnable {
		private static final String monitorUrl = "localhost";
		private static final int monitorPort = 6422;
		private static final long updateInterval = 10000;
		private static Socket socket;

		@Override
		public void run() {
			System.out.println("[DSSMaster] Getting nodes from " + monitorUrl);
			while (true) {
				BufferedReader in = null;
				PrintWriter out = null;
				Gson gson = new Gson(); 

				try {
					socket = new Socket(monitorUrl, monitorPort);

					in = new BufferedReader(new InputStreamReader (socket.getInputStream()));
					out = new PrintWriter(socket.getOutputStream(), true);

					// Send GET storage nodes request to NebulaMonitor
					NodeRequest request = new NodeRequest(null, NodeRequestType.STORAGE);
					out.println(gson.toJson(request));
					out.flush();

					// Parse the list of online storage nodes
					HashMap<String, NodeInfo> onlineNodes = gson.fromJson(in.readLine(), 
							new TypeToken<HashMap<String, NodeInfo>>(){}.getType());

					if (onlineNodes != null) {
						synchronized (nodesLock) {
							storageNodes = onlineNodes;
						}
					}
					in.close();
					out.close();
					socket.close();
					Thread.sleep(updateInterval);
				} catch (IOException  e) {
					System.out.println("[DSSMaster] Failed connecting to Nebula Monitor: " + e);
					e.printStackTrace();
					return;
				} catch (InterruptedException e1) {
					System.out.println("[DSSMaster] Sleep interrupted: " + e1);
				}
			}
		}
	}
		 */

	private static class DSSThread implements Runnable {
		private final Socket clientSock;

		public DSSThread(Socket sock) {
			clientSock = sock;
		}

		@Override
		public void run() {
			BufferedReader in = null;
			PrintWriter out = null;

			try {
				in = new BufferedReader(new InputStreamReader(clientSock.getInputStream()));
				out = new PrintWriter(clientSock.getOutputStream());

				String request = in.readLine();

				DSSRequest dssRequest = gson.fromJson(request, DSSRequest.class);
				NodeRequest nodeRequest = gson.fromJson(request, NodeRequest.class);
				HashMap<String, NodeInfo> nodes = null;
				boolean success = false;

				if (nodeRequest != null) {
					NodeInfo node = nodeRequest.getNode();
					
					switch (nodeRequest.getType()) {
					case ONLINE: // online heartbeat
						storageNodes.put(node.getId(), node);
						break;
					case OFFLINE: // offline heartbeat
						storageNodes.remove(node.getId());
						break;
					default:
						System.out.println("[DSSMaster] Invalid Node request: " + nodeRequest.getType());
					}
					out.println(gson.toJson(success));	
				}

				else if (dssRequest != null) {
					switch (dssRequest.getType()) {
					case GETNODES:	// get a list of nodes
						nodes = getNodes(0);
						out.println(gson.toJson(nodes)); 
						break;
					case GETNODESWITHFILE: // get a list of nodes storing a specific file
						if (dssRequest.getNamespace() != null && dssRequest.getFilename() != null) {
							nodes = getStorageNodesWithFile(dssRequest.getNamespace(), dssRequest.getFilename());
							out.println(gson.toJson(nodes));
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
						System.out.println("[DSSMaster] Invalid DSS request: " + dssRequest.getType());
						out.println(gson.toJson(success));
					}
				}

				else {
					out.println(gson.toJson(success));
				}
				out.flush();
			} catch (IOException e) {
				System.err.println("[DSSMaster]: " + e);
			} finally {
				try {
					if (in != null) in.close();
					if (out != null) out.close();
					if (clientSock != null) clientSock.close();
				} catch (IOException e) {
					System.err.println("[DSSMaster] Failed to close streams or socket: " + e);
				}
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
		return storageNodes;
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
				result.put(id, storageNodes.get(id));
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

		System.out.println("NEW: " + file + " in node: " + nodeId);
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
