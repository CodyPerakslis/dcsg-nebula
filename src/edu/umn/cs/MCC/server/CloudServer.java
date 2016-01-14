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
	private static final long maxInactive = 5 * 60 * 1000;

	private static Object nodesLock = new Object();
	private static HashMap<String, Node> primaryNodes = new HashMap<String, Node>();
	private static HashMap<Location, HashMap<String, Double>> locationNodesMap = new HashMap<Location, HashMap<String, Double>>();

	public static String getNode(Node mobileUser) {
		String nodeId = null;
		Double bestValue = 0.0;
		Location loc = new Location(gridSize, mobileUser.getLatitude(), mobileUser.getLongitude());

		// get node based on location
		if (locationNodesMap.containsKey(loc)) {
			// find the best node
			for (Entry<String, Double> node: locationNodesMap.get(loc).entrySet()) {
				if (bestValue <= node.getValue()) {
					nodeId = node.getKey();
					bestValue = node.getValue();
				}
			}
			// update the node's score
			if (nodeId == null)
				return null;

			if (bestValue == Double.MAX_VALUE) {
				for (Entry<String, Double> node: locationNodesMap.get(loc).entrySet()) {
					locationNodesMap.get(loc).put(node.getKey(), 0.0);
				}
			}

			bestValue += 1;
			locationNodesMap.get(loc).put(nodeId, bestValue);

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
	
	private static class NodeMonitorThread implements Runnable {
		private Date now = new Date();
		private Set<String> removedNodes = new HashSet<String>();

		@Override
		public void run() {
			while (true) {
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
					Thread.sleep(60 * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
	}

	private static class RequestThread implements Runnable {
		private final Socket clientSock;

		public RequestThread(Socket sock) {
			clientSock = sock;
		}

		private HashMap<String, Node> handleNodeRequest(NodeRequest request) {
			Node node = request.getNode();
			HashMap<String, Node> result = null;
			
			if (node == null) {
				System.out.println("Invalid node.");
				return null;
			}
			
			switch (request.getType()) {
			case ONLINE:
				System.out.println("Online: " + node.getId());
				synchronized (nodesLock) {
					if (primaryNodes.containsKey(node.getId())) {
						primaryNodes.get(node.getId()).updateLastOnline();
					} else {
						primaryNodes.put(node.getId(), node);
					}
					result = primaryNodes;
				}
				result.remove(node.getId());
				break;
			case OFFLINE:
				System.out.println("Offline: " + node.getId());
				synchronized (nodesLock) {
					primaryNodes.remove(node.getId());
					result = primaryNodes;
				}
				result.remove(node.getId());
				break;
			default:
				System.out.println("Invalid node request type: " + request.getType());
			}
			return result;
		}

		private String handleMobileRequest(MobileRequest request) {
			String status = "failed";
			
			switch (request.getType()) {
			case LOCATION:
				Node mobileUser = new Node(request.getUserId(), 
						clientSock.getRemoteSocketAddress().toString(), 
						request.getLatitude(), request.getLongitude(), NodeType.MOBILE);
				String node = getNode(mobileUser);
				status = node;
				break;
			default:
				System.out.println("Undefined request type: " + request.getType());
			}
			return status;
		}

		@Override
		public void run() {
			BufferedReader in = null;
			PrintWriter out = null;
			String response = "failed";
			Gson gson = new Gson();

			System.out.print("Received a request of type: ");
			try {
				in = new BufferedReader(new InputStreamReader (clientSock.getInputStream()));
				out = new PrintWriter(clientSock.getOutputStream(), true);

				String input = in.readLine();
				NodeRequest nodeRequest = gson.fromJson(input, NodeRequest.class);
				MobileRequest mobilRequest = gson.fromJson(input, MobileRequest.class);

				if (nodeRequest != null) {
					System.out.println("nodeRequest.");
					HashMap<String, Node> nodes = handleNodeRequest(nodeRequest);
					out.println(gson.toJson(nodes));
				} else if (mobilRequest != null) {
					System.out.println("mobilRequest.");
					response = handleMobileRequest(mobilRequest);
					out.println(gson.toJson(response));
				} else {
					System.out.println("Invalid request.");
					out.println(gson.toJson(response));
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
	}
}
