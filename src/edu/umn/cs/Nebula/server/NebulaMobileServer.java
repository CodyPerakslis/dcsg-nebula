package edu.umn.cs.Nebula.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.gson.Gson;

import edu.umn.cs.Nebula.model.Location;
import edu.umn.cs.Nebula.node.NodeInfo;
import edu.umn.cs.Nebula.node.NodeType;
import edu.umn.cs.Nebula.request.MobileRequest;

public class NebulaMobileServer {

	private static final int port = 6424;
	private static final int poolSize = 50;
	private static final int gridSize = 10;

	private static HashMap<String, NodeInfo> primaryNodes = new HashMap<String, NodeInfo>();
	private static HashMap<Integer, HashMap<Integer, HashMap<String, Double>>> locationNodesMap = new HashMap<>();
	// <x, <y, <id, score>>>

	/**
	 * Get a primary node for a mobileUser. 
	 * The implementation of this method may be different depending on the application's needs.
	 * 
	 * @param mobileUser
	 * @return
	 */
	public static String getNode(NodeInfo mobileUser) {
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
			// there is no node found in the same location as the user, find a node from the other locations
			// starting with the neighboring location.
			int i = 1;
			do {
				if (x-i >= 0) {
					// (x-i, y-i)
					if (y-i >= 0) {
						for (Entry<String, Double> node: locationNodesMap.get(x-i).get(y-i).entrySet()) {
							if (bestScore < 0 || bestScore > node.getValue()+i) {
								nodeId = node.getKey();
								bestScore = node.getValue() + i;
							}
						}
					}
					// (x-i, y)
					for (Entry<String, Double> node: locationNodesMap.get(x-i).get(y).entrySet()) {
						if (bestScore < 0 || bestScore > node.getValue()+i) {
							nodeId = node.getKey();
							bestScore = node.getValue() + i;
						}
					}
					// (x-i, y+i)
					if (y+i < gridSize) {
						for (Entry<String, Double> node: locationNodesMap.get(x-i).get(y+i).entrySet()) {
							if (bestScore < 0 || bestScore > node.getValue()+i) {
								nodeId = node.getKey();
								bestScore = node.getValue() + i;
							}
						}
					}
				}

				// (x, y-i)
				if (y-i >= 0) {
					for (Entry<String, Double> node: locationNodesMap.get(x).get(y-i).entrySet()) {
						if (bestScore < 0 || bestScore > node.getValue()+i) {
							nodeId = node.getKey();
							bestScore = node.getValue() + i;
						}
					}
				}
				// (x, y+i)
				if (y+i < gridSize) {
					for (Entry<String, Double> node: locationNodesMap.get(x).get(y+i).entrySet()) {
						if (bestScore < 0 || bestScore > node.getValue()+i) {
							nodeId = node.getKey();
							bestScore = node.getValue() + i;
						}
					}
				}

				if (x+i < gridSize) {
					// (x+i, y-i)
					if (y-i >= 0) {
						for (Entry<String, Double> node: locationNodesMap.get(x+i).get(y-i).entrySet()) {
							if (bestScore < 0 || bestScore > node.getValue()+i) {
								nodeId = node.getKey();
								bestScore = node.getValue() + i;
							}
						}
					}
					// (x+i, y)
					for (Entry<String, Double> node: locationNodesMap.get(x+i).get(y).entrySet()) {
						if (bestScore < 0 || bestScore > node.getValue()+i) {
							nodeId = node.getKey();
							bestScore = node.getValue() + i;
						}
					}
					// (x+i, y+i)
					if (y+i < gridSize) {
						for (Entry<String, Double> node: locationNodesMap.get(x+i).get(y+i).entrySet()) {
							if (bestScore < 0 || bestScore > node.getValue()+i) {
								nodeId = node.getKey();
								bestScore = node.getValue() + i;
							}
						}
					}
				}
				i++;
			} while (i < gridSize && nodeId != null); // terminate when a node is found in the neighboring grid cell
		}
		if (nodeId == null) {
			System.out.println("No online node found in all location.");
		}

		return nodeId;
	}

	public static void main(String[] args) {
		// Listening for client requests
		ExecutorService requestPool = Executors.newFixedThreadPool(poolSize);
		ServerSocket serverSock = null;
		try {
			serverSock = new ServerSocket(port);
			System.out.println("[MOBILE] Listening for requests on port " + port);
			while (true) {
				requestPool.submit(new RequestThread(serverSock.accept()));
			}
		} catch (IOException e) {
			System.err.println("[MOBILE] Failed to establish listening socket: " + e);
		} finally {
			requestPool.shutdown();
			if (serverSock != null) {
				try {
					serverSock.close();
				} catch (IOException e) {
					System.err.println("[MOBILE] Failed to close listening socket");
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

				MobileRequest mobileRequest = gson.fromJson(input, MobileRequest.class);

				if (mobileRequest != null) {
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
				NodeInfo mobileUser = new NodeInfo(request.getUserId(), 
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
