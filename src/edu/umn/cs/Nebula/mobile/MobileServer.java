package edu.umn.cs.Nebula.mobile;

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
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import edu.umn.cs.Nebula.model.NodeInfo;
import edu.umn.cs.Nebula.request.ComputeRequest;
import edu.umn.cs.Nebula.request.MobileRequest;
import edu.umn.cs.Nebula.request.SchedulerRequest;
import edu.umn.cs.Nebula.request.SchedulerRequestType;

public class MobileServer {
	private static final Gson gson = new Gson();
	private static final int requestPoolSize = 20;
	private static final String resourceManager = "localhost";
	private static final int resourceManagerPort = 6424;
	private static final int requestPort = 6429;
	private static final int nodePort = 6425;
	private static final Object nodesLock = new Object();

	private static boolean success = false;
	private static HashMap<String, NodeInfo> nodes;
	private static LinkedList<String> activeNodes;
	private static Grid index;

	public static void main(String[] args) throws InterruptedException {
		nodes = new HashMap<String, NodeInfo>();	
		activeNodes = new LinkedList<String>();
		index = new Grid(24, 25, 49, -125, -65);

		// Connecting to Nebula Server
		Thread nodeThread = new Thread(new NebulaThread());
		nodeThread.start();

		int counter = 0;
		int maxCounter = 5;
		while (!success) {
			if (counter >= maxCounter) {
				nodeThread.interrupt();
				System.out.println("[MOBILE] Failed connecting to Nebula Server at: " + resourceManager);
				System.exit(1);
			}
			Thread.sleep(1000);
			counter++;
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
	 * Thread class used to connect and get a list of available nodes from Nebula Resource Manager.
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
					System.out.println("[MOBILE] Failed connecting to the Nebula Resource Manager: " + e.getMessage());
					return;
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
							if (!nodes.containsKey(nodeId)) {
								activeNodes.add(nodeId);
								index.insertItem(nodeId, temp.get(nodeId).getLatitude(), temp.get(nodeId).getLongitude());
							}
							nodes.put(nodeId, temp.get(nodeId));
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
					System.out.println("[MOBILE] Available nodes: " + activeNodes);
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
			MobileRequest mRequest = null;
			ComputeRequest cRequest = null;
			double latitude, longitude;

			try {
				in = new BufferedReader(new InputStreamReader (clientSock.getInputStream()));
				out = new PrintWriter(clientSock.getOutputStream(), true);

				// read the input message and parse it as a node request
				input = in.readLine();
				mRequest = gson.fromJson(input, MobileRequest.class);
				cRequest = gson.fromJson(input, ComputeRequest.class);
			} catch (IOException e) {
				System.err.println("[MOBILE] Error: " + e.getMessage());
			}

			if (mRequest != null && mRequest.getType() != null) {
				// System.out.println("[MOBILE] Handling MobileRequest of type " + mRequest.getType());
				if (!activeNodes.isEmpty()) {
					switch (mRequest.getType()) {
					case LOCAL:
						// return a list of nodes that are located within the same cell as the client
						if (mRequest.getNode() != null) {
							latitude = mRequest.getNode().getLatitude();
							longitude = mRequest.getNode().getLongitude();
							LinkedList<String> temp = index.getItems(index.getGridLocation(latitude, longitude));
							for (String node: temp) {
								if (activeNodes.contains(node))
									result.add(node);
							}
							if (result.isEmpty()) {
								Random rand = new Random();
								result.add(activeNodes.get(rand.nextInt(activeNodes.size()-1)));
							}
						}
						break;
					case ALL:
						// return all available nodes
						result.addAll(activeNodes);
						break;
					default:
						System.out.println("[MOBILE] Undefined request type: " + mRequest.getType());
						result.add("Mobile Request Failed");
					}
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
							Random rand = new Random();
							status = sendRequestToNode(activeNodes.get(rand.nextInt(activeNodes.size()-1)), cRequest);
						}
					} else if (activeNodes.contains(cRequest.getIp())) {
						status = sendRequestToNode(cRequest.getIp(), cRequest);
					}
					result.add(status);
					break;
				case STOPAPP:
					if (cRequest.getIp() != null && cRequest.getContents() != null && !cRequest.getContents().isEmpty()) {
						status = sendRequestToNode(cRequest.getIp(), cRequest);
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

		private static String sendRequestToNode(String nodeIp, ComputeRequest request) {
			PrintWriter out = null;
			BufferedReader in = null;
			String result = "Failed";

			try {
				Socket socket = new Socket(nodeIp, nodePort);
				out = new PrintWriter(socket.getOutputStream(), true);
				out.println(gson.toJson(request));
				out.flush();

				in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				result = in.readLine();

				out.close();
				in.close();
				socket.close();
			} catch (IOException e) {
				System.out.println("Failed connecting to the Mobile Node: " + e.getMessage());
			}
			return result;
		}

	}
}
