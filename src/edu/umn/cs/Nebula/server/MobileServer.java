package edu.umn.cs.Nebula.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import edu.umn.cs.Nebula.mobile.Grid;
import edu.umn.cs.Nebula.model.NodeInfo;
import edu.umn.cs.Nebula.request.ComputeRequest;
import edu.umn.cs.Nebula.request.MobileRequest;
import edu.umn.cs.Nebula.request.SchedulerRequest;
import edu.umn.cs.Nebula.request.SchedulerRequestType;

public class MobileServer {
	private static final Gson gson = new Gson();
	private static boolean success = false;
	private static final int requestPoolSize = 20;

	private static final String resourceManager = "localhost";
	private static final int resourceManagerPort = 6426;
	private static final int requestPort = 6429;
	private static final int nodePort = 6425;

	private static HashMap<String, NodeInfo> nodes;
	private static LinkedList<String> activeNodes;
	private static Grid index;

	private static final Object nodesLock = new Object();

	public static void main(String[] args) throws InterruptedException {
		nodes = new HashMap<String, NodeInfo>();	
		activeNodes = new LinkedList<String>();
		index = new Grid(24, 25, 49, -125, -65);

		int counter = 0;
		int maxCounter = 5;

		// Connecting to Nebula Server
		Thread nodeThread = new Thread(new NebulaThread());
		nodeThread.start();
		while (!success) {
			Thread.sleep(1000);
			counter++;
			if (counter == maxCounter) {
				nodeThread.interrupt();
				System.out.println("[MOBILE] Failed connecting to Nebula Server at: " + resourceManager);
				System.exit(0);
			}
		}

		// Listening for client requests
		ExecutorService requestPool = Executors.newFixedThreadPool(requestPoolSize);
		ServerSocket serverSock = null;
		try {
			serverSock = new ServerSocket(requestPort);
			System.out.println("[MOBILE] Listening for requests on port " + requestPort);
			while (true) {
				requestPool.submit(new ClientThread(serverSock.accept()));
			}
		} catch (IOException e) {
			System.out.println("[MOBILE] Server Thread exits: " + e.getMessage());
			nodeThread.interrupt();
			return;
		}
	}

	/**
	 * Thread class used to connect to Nebula Server.
	 * This thread periodically update the list of online nodes from the Nebula Server.
	 * 
	 * @author albert
	 */
	private static class NebulaThread implements Runnable {
		private final int interval = 5000; // in milliseconds
		private boolean isFirst = true;

		@Override
		public void run() {
			HashMap<String, NodeInfo> temp = new HashMap<String, NodeInfo>();
			PrintWriter out = null;
			BufferedReader in = null;
			Socket socket = null;
			SchedulerRequest request = new SchedulerRequest(SchedulerRequestType.GET, "MOBILE");
			HashSet<String> removedNodes = new HashSet<String>();

			while (true) {
				try {
					socket = new Socket(resourceManager, resourceManagerPort);
					out = new PrintWriter(socket.getOutputStream(), true);
					out.println(gson.toJson(request));
					out.flush();
					in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					temp = gson.fromJson(in.readLine(), new TypeToken<HashMap<String, NodeInfo>>(){}.getType());
				} catch (IOException e) {
					System.out.println("[MOBILE] Failed connecting to the Nebula Monitor: " + e.getMessage());
					try {
						Thread.sleep(interval);
					} catch (InterruptedException e1) {
						System.out.println("[MOBILE] Nebula Thread exits: " + e1.getMessage());
						return;
					}
				}
				
				try {
					out.close();
					in.close();
					if (socket != null) socket.close();
				} catch (IOException e) {
					System.out.println("[MOBILE] Failed closing streams: " + e.getMessage());
				}

				synchronized (nodesLock) {
					for (String nodeId: temp.keySet()) {
						if (temp.get(nodeId).getNote().equalsIgnoreCase("available")) {
							nodes.put(nodeId, temp.get(nodeId));
							activeNodes.add(nodeId);
							index.insertItem(nodeId, temp.get(nodeId).getLatitude(), temp.get(nodeId).getLongitude());
						}
					}	
					// remove nodes that become inactive
					for (String nodeId: nodes.keySet()) {
						if (!temp.containsKey(nodeId) || !temp.get(nodeId).getNote().equalsIgnoreCase("available")) {
							removedNodes.add(nodeId);
						}
					}
					for (String nodeId: removedNodes) {
						index.removeItem(nodeId, nodes.get(nodeId).getLatitude(), nodes.get(nodeId).getLongitude());
						activeNodes.remove(nodeId);
						nodes.remove(nodeId);
					}
				}
				removedNodes.clear();
				
				if (isFirst) {
					success = true;
					isFirst = false;
					System.out.println("[MOBILE] Connected to Nebula Server.");
				} else {
					System.out.println("[MOBILE] Available nodes: " + nodes.keySet());
				}

				try {
					Thread.sleep(interval);
				} catch (InterruptedException e) {
					System.out.println("[MOBILE] Nebula Thread exits: " + e.getMessage());
					return;
				}
			}
		}
	}

	private static String sendApplicationToNode(String node, ComputeRequest request) {
		PrintWriter out = null;
		BufferedReader in = null;
		String result = "Failed";

		try {
			Socket socket = new Socket(node, nodePort);
			out = new PrintWriter(socket.getOutputStream(), true);
			out.println(gson.toJson(request));
			out.flush();

			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			result = in.readLine();

			out.close();
			in.close();
			socket.close();
		} catch (IOException e) {
			System.out.println("Failed connecting to the Mobile Server: " + e);
		}
		return result;
	}
	
	/**
	 * Thread class used to serve a client's request.
	 * 
	 * @author albert
	 */
	private static class ClientThread implements Runnable {
		private final Socket clientSock;

		public ClientThread(Socket socket) {
			clientSock = socket;
		}

		@Override
		public void run() {
			BufferedReader in = null;
			PrintWriter out = null;
			ArrayList<String> result = new ArrayList<String>();
			String input;
			MobileRequest request = null;
			ComputeRequest cRequest = null;
			double latitude, longitude;

			try {
				in = new BufferedReader(new InputStreamReader (clientSock.getInputStream()));
				out = new PrintWriter(clientSock.getOutputStream(), true);

				// read the input message and parse it as a node request
				input = in.readLine();
				request = gson.fromJson(input, MobileRequest.class);
				cRequest = gson.fromJson(input, ComputeRequest.class);
			} catch (IOException e) {
				System.err.println("[MOBILE] Error: " + e);
			}

			if (request != null && request.getType() != null) {
				// TODO handle client's request
				// System.out.println("[MOBILE] Handling MobileRequest of type " + request.getType());
				switch (request.getType()) {
				case LOCAL:
					if (request.getNode() != null) {
						latitude = request.getNode().getLatitude();
						longitude = request.getNode().getLongitude();
						LinkedList<String> temp = index.getItems(index.getGridLocation(latitude, longitude));
						for (String node: temp) {
							if (activeNodes.contains(node))
								result.add(node);
						}
					}
					break;
				case ALL:
					result.addAll(activeNodes);
					break;
				default:
					System.out.println("[MOBILE] Undefined request type: " + request.getType());
					result.add("Mobile Request Failed");
				}
			} else if (cRequest != null && cRequest.getRequestType() != null) {
				// System.out.println("[MOBILE] Handling ComputeRequest of type " + cRequest.getRequestType());
				String status = "Failed";
				// TODO handle job submission
				switch (cRequest.getRequestType()) {
				case RUNAPP:
					if (cRequest.getContents() == null || cRequest.getContents().isEmpty()) {
						// invalid request
					} else if (cRequest.getIp().isEmpty() || cRequest.getIp().equals("")) {
						// send to any node
						if (activeNodes.isEmpty()) {
							System.out.println("[MOBILE] No available nodes");
							status = "Nodes not available";
						} else {
							status = sendApplicationToNode(activeNodes.getFirst(), cRequest);
						}
					} else {
						// TODO implement some policy
					}
					result.add(status);
					break;
				case STOPAPP:
					if (cRequest.getIp() != null && cRequest.getContents() != null && !cRequest.getContents().isEmpty()) {
						status = sendApplicationToNode(cRequest.getIp(), cRequest);
					}
					result.add(status);
					break;
				default:
					System.out.println("[MOBILE] Undefined compute request type: " + cRequest.getRequestType());
					result.add("Compute Request Failed");
				}
			} else {
				System.out.println("[MOBILE] Request not found or invalid.");
				result.add("Invalid request");
			}
			out.println(gson.toJson(result));
			out.flush();

			try {
				if (in != null) in.close();
				if (out != null) out.close();
				if (clientSock != null) clientSock.close();
			} catch (IOException e) {
				System.err.println("[MOBILE] Failed to close streams or socket: " + e);
			}
		}
	}
}
