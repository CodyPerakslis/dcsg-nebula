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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import edu.umn.cs.Nebula.model.NodeInfo;
import edu.umn.cs.Nebula.request.MobileRequest;
import edu.umn.cs.Nebula.request.NodeRequest;
import edu.umn.cs.Nebula.request.NodeRequestType;

public class MobileServer {
	private static final Gson gson = new Gson();
	private static boolean success = false;
	private static final int requestPoolSize = 20;

	private static String nebulaMonitorServer = "localhost";
	private static final int nebulaMonitorPort = 6422;
	private static int port = 6429;

	private static HashMap<String, NodeInfo> nodes;
	private static Grid index;

	private static final Object nodesLock = new Object();


	public static void main(String[] args) throws InterruptedException {
		nodes = new HashMap<String, NodeInfo>();		
		index = new Grid(24, 25, 49, -125, -65);

		int counter = 0;
		int maxCounter = 5;

		if (args.length < 2) {
			System.out.println("[MOBILE] Parameters: nebulaMonitorServer, port");
			System.exit(1);
		} else {
			nebulaMonitorServer = args[0];
			port = Integer.parseInt(args[1]);
		}

		// Connecting to Nebula Server
		Thread nodeThread = new Thread(new NebulaThread());
		nodeThread.start();
		while (!success) {
			Thread.sleep(1000);
			counter++;
			if (counter == maxCounter) {
				nodeThread.interrupt();
				System.out.println("[MOBILE] Failed connecting to Nebula Server at: " + nebulaMonitorServer);
				System.exit(0);
			}
		}

		// Listening for client requests
		ExecutorService requestPool = Executors.newFixedThreadPool(requestPoolSize);
		ServerSocket serverSock = null;
		try {
			serverSock = new ServerSocket(port);
			System.out.println("[MOBILE] Listening for requests on port " + port);
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
			Socket socket;
			NodeRequest nodeRequest = new NodeRequest(NodeRequestType.COMPUTE);
			HashSet<String> removedNodes = new HashSet<String>();

			while (true) {
				try {
					socket = new Socket(nebulaMonitorServer, nebulaMonitorPort);
					out = new PrintWriter(socket.getOutputStream(), true);
					nodeRequest.setType(NodeRequestType.COMPUTE);
					out.println(gson.toJson(nodeRequest));
					out.flush();
					in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					temp = gson.fromJson(in.readLine(), new TypeToken<HashMap<String, NodeInfo>>(){}.getType());

					synchronized (nodesLock) {
						for (String nodeId: temp.keySet()) {
							nodes.put(nodeId, temp.get(nodeId));
							index.insertItem(nodeId, temp.get(nodeId).getLatitude(), temp.get(nodeId).getLongitude());
						}
						// remove nodes that become inactive
						for (String nodeId: nodes.keySet()) {
							if (!temp.containsKey(nodeId)) {
								removedNodes.add(nodeId);
							}
						}
						for (String nodeId: removedNodes) {
							index.removeItem(nodeId, nodes.get(nodeId).getLatitude(), nodes.get(nodeId).getLongitude());
							nodes.remove(nodeId);
						}
					}
					removedNodes.clear();
					out.close();
					in.close();
					socket.close();
					
					if (isFirst) {
						success = true;
						isFirst = false;
						System.out.println("[MOBILE] Connected to Nebula Server.");
					} else {
						System.out.println("\n[MOBILE] Compute nodes: " + nodes.keySet());
					}
				} catch (IOException e) {
					System.out.println("[MOBILE] Failed connecting to the Nebula Monitor: " + e);
				}

				try {
					Thread.sleep(interval);
				} catch (InterruptedException e) {
					System.out.println("[MOBILE] Nebula Thread exits.");
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
			ArrayList<String> temp = new ArrayList<String>();

			try {
				in = new BufferedReader(new InputStreamReader (clientSock.getInputStream()));
				out = new PrintWriter(clientSock.getOutputStream(), true);

				// read the input message and parse it as a node request
				String input = in.readLine();
				MobileRequest request = gson.fromJson(input, MobileRequest.class);
				double latitude, longitude;

				if (request != null) {
					// TODO handle client's request
					switch (request.getType()) {
					case LOCAL:
						latitude = request.getNode().getLatitude();
						longitude = request.getNode().getLongitude();
						if (request.getNode() != null)
							temp.addAll(index.getItems(index.getGridLocation(latitude, longitude)));
						break;
					case ALL:
						temp.addAll(nodes.keySet());
						break;
					default:
						System.out.println("[MOBILE] Undefined request type: " + request.getType());
					}
					out.println(gson.toJson(temp));
				} else {
					System.out.println("[MOBILE] Request not found or invalid.");
					out.println(gson.toJson(temp));
				}
				out.flush();
			} catch (IOException e) {
				System.err.println("[MOBILE] Error: " + e);
			} finally {
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
}
