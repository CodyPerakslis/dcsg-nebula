package edu.umn.cs.Nebula.instance;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.gson.Gson;

import edu.umn.cs.Nebula.model.DatabaseConnector;
import edu.umn.cs.Nebula.model.NodeInfo;
import edu.umn.cs.Nebula.model.NodeType;
import edu.umn.cs.Nebula.request.NodeRequest;

public class NebulaMonitor {
	private static final int port = 6422;
	private static final int poolSize = 50;

	private static HashMap<String, NodeInfo> computeNodes = new HashMap<String, NodeInfo>();
	private static HashMap<String, NodeInfo> storageNodes = new HashMap<String, NodeInfo>();
	private static Gson gson = new Gson();

	public static void main(String args[]) {
		// Listening for client requests
		ExecutorService requestPool = Executors.newFixedThreadPool(poolSize);
		ServerSocket serverSock = null;
		try {
			serverSock = new ServerSocket(port);
			System.out.println("[MONITOR] Listening for requests on port " + port);
			while (true) {
				requestPool.submit(new MonitorRequestHandler(serverSock.accept()));
			}
		} catch (IOException e) {
			System.err.println("[MONITOR] Failed to establish listening socket: " + e);
		} finally {
			requestPool.shutdown();
			if (serverSock != null) {
				try {
					serverSock.close();
				} catch (IOException e) {
					System.err.println("[MONITOR] Failed to close listening socket");
				}
			}
		}
	}

	/**
	 * This thread class will handle any request from the node/end-user.
	 * 
	 * @author albert
	 *
	 */
	private static class MonitorRequestHandler implements Runnable {
		private final Socket clientSock;

		public MonitorRequestHandler(Socket sock) {
			clientSock = sock;
		}

		@Override
		public void run() {
			BufferedReader in = null;
			PrintWriter out = null;
			HashMap<String, NodeInfo> result = new HashMap<String, NodeInfo>();

			try {
				in = new BufferedReader(new InputStreamReader (clientSock.getInputStream()));
				out = new PrintWriter(clientSock.getOutputStream(), true);

				// read the input message and parse it as a node request
				String input = in.readLine();
				NodeRequest nodeRequest = gson.fromJson(input, NodeRequest.class);

				if (nodeRequest != null) {
					switch (nodeRequest.getType()) {
					case COMPUTE:	// handle get a list of compute nodes
						result.putAll(computeNodes);
						out.println(gson.toJson(result));
						break;
					case STORAGE:	// handle get a list of storage nodes
						result.putAll(storageNodes);
						out.println(gson.toJson(result));
						break;
					default:
						System.out.println("[MONITOR] Invalid node request type: " + nodeRequest.getType());
						out.println(gson.toJson(result));
					}
				} else {
					System.out.println("[MONITOR] Request not found or invalid.");
					out.println(gson.toJson(result));
				}
				out.flush();
			} catch (IOException e) {
				System.err.println("[MONITOR] Error: " + e);
			} finally {
				try {
					if (in != null) in.close();
					if (out != null) out.close();
					if (clientSock != null) clientSock.close();
				} catch (IOException e) {
					System.out.println("[MONITOR] Failed to close streams or socket: " + e);
				}
			}
		}
	}
}
