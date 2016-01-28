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

import redis.clients.jedis.Jedis;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import edu.umn.cs.Nebula.node.NodeInfo;
import edu.umn.cs.Nebula.request.DSSRequest;
import edu.umn.cs.Nebula.request.NodeRequest;
import edu.umn.cs.Nebula.request.NodeRequestType;

public class NebulaDSSMaster {

	private static final int port = 6423;
	private static final int poolSize = 10;

	private static Jedis jedis;
	private static final String jedisServer = "localhost";
	private static final int jedisPort = 6379;

	private static HashMap<String, NodeInfo> storageNodes;
	private static final Object nodesLock = new Object();

	public static void main(String args[]) {
		System.out.println("[DSSMaster] Initizalizing redis connection");
		storageNodes = new HashMap<String, NodeInfo>();
		jedis = new Jedis(jedisServer, jedisPort);
		jedis.connect();

		System.out.println("[DSSMaster] Connecting to Monitor");
		Thread monitorThread = new Thread(new MonitorThread());
		monitorThread.start();

		ExecutorService requestPool = Executors.newFixedThreadPool(poolSize);
		ServerSocket serverSock = null;

		try {
			serverSock = new ServerSocket(port);
			System.out.println("[DSSMaster] Listening for DSS requests on port " + port);
			while (true) {
				requestPool.submit(new RequestThread(serverSock.accept()));
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
	 */
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

	private static class RequestThread implements Runnable {
		private final Socket clientSock;

		public RequestThread(Socket sock) {
			clientSock = sock;
		}

		@Override
		public void run() {
			BufferedReader in = null;
			PrintWriter out = null;
			Gson gson = new Gson();

			try {
				in = new BufferedReader(new InputStreamReader(clientSock.getInputStream()));
				out = new PrintWriter(clientSock.getOutputStream());

				DSSRequest request = gson.fromJson(in.readLine(), DSSRequest.class);
				HashMap<String, NodeInfo> nodes = null;
				boolean success = false;

				switch (request.getType()) {
				case GETNODES:
					// this message should be sent by the end-user
					nodes = getNodes();
					out.println(gson.toJson(nodes)); break;
				case GETNODESWITHFILE:
					// this message should be sent by the end-user
					if (request.getNamespace() != null && request.getFilename() != null) {
						nodes = getStorageNodesWithFile(request.getNamespace(), request.getFilename());
						out.println(gson.toJson(nodes));
					} break;
				case NEW:
					// this message should be sent by a storage node, notifying a new file has been stored in the node
					if (request.getNamespace() != null && request.getFilename() != null && request.getNodeId() != null) {
						success = newFile(request.getNamespace(), request.getFilename(), request.getNodeId());
					}
					out.println(gson.toJson(success)); break;
				case DELETE:
					// this message should be sent by a storage node, notifying a file has been deleted in the node
					if (request.getNamespace() != null && request.getFilename() != null && request.getNodeId() != null) {
						success = deleteFile(request.getNamespace(), request.getFilename(), request.getNodeId());
					}
					out.println(gson.toJson(success)); break;
				default:
					System.out.println("[DSSMaster] Invalid request: " + request.getType());
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
	private static HashMap<String, NodeInfo> getNodes() {
		return storageNodes;
	}

	/**
	 * Get a list of storage nodes that contain a specific file.
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
