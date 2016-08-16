package edu.umn.cs.Nebula.node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.gson.Gson;

import edu.umn.cs.Nebula.model.JobType;
import edu.umn.cs.Nebula.request.ComputeRequest;
import edu.umn.cs.Nebula.request.ComputeRequestType;

public class ComputeNode extends Node {
	private static final int max_failure = 5;
	private static final int interval = 2000; // in milliseconds
	private static final int poolSize = 10;
	private static final int requestPort = 2020;
	private static final String nebulaUrl = "http://hemant-umh.cs.umn.edu:6420/NebulaCentral/NodeHandler";

	private static Gson gson = new Gson();

	/**
	 * Make sure that the node is ready to run, i.e., it has its own id.
	 * @throws InterruptedException
	 */
	private static boolean isReady() throws InterruptedException {
		int counter = 0;
		while (id == null) {
			if (counter < max_failure) {
				counter++;
				Thread.sleep(interval);
			} else {
				System.out.println("[COMPUTE] Failed getting node id. Exiting.");
				return false;
			}
		}
		return true;
	}

	public static void main(String args[]) throws InterruptedException {
		connect(nebulaUrl, NodeType.COMPUTE);
		if (!isReady()) return;

		while (true) {
			runServerTask();
		}
	}

	/**
	 * Here, the compute node is performing as a client-server that listens to end users on a specific port.
	 */
	private static void runServerTask() {
		ExecutorService requestPool = Executors.newFixedThreadPool(poolSize);
		ServerSocket serverSock = null;

		try {
			serverSock = new ServerSocket(requestPort);
			System.out.println("[COMPUTE] Listening for client requests on port " + requestPort);
			while (true) {
				// listen for client requests
				requestPool.submit(new RequestHandler(serverSock.accept()));
			}
		} catch (IOException e) {
			System.err.println("[COMPUTE] Failed listening: " + e);
		}
	}

	/**
	 * This thread is invoked when the node receives a request.
	 * Handle the request depending on the type of the task.
	 */
	public static class RequestHandler implements Runnable {
		private final Socket clientSock;
		private static final int timeout = 10000;
		private long latency = -1;

		private RequestHandler(Socket sock) {
			clientSock = sock;
		}

		private String handleMobileRequest(ComputeRequest request) {
			String result = "";
			ComputeRequestType requestType = request.getRequestType();

			if (requestType.equals(ComputeRequestType.PING)) {
				System.out.println("[COMPUTE] PING from " + request.getMyIp() + " (" + latency + " ms)");
				ComputeRequest reply = new ComputeRequest(request.getNodeIp(), JobType.MOBILE, ComputeRequestType.PING);
				reply.setNodeIp(request.getNodeIp());
				reply.setTimestamp(System.currentTimeMillis());
				return gson.toJson(reply);
			}
			
			return result;
		}

		@Override
		public void run() {
			BufferedReader in = null;
			PrintWriter out = null;
			String reply = "";

			try {		
				in = new BufferedReader(new InputStreamReader(clientSock.getInputStream()));
				out = new PrintWriter(clientSock.getOutputStream());
				clientSock.setSoTimeout(timeout);

				ComputeRequest request;
				request = gson.fromJson(in.readLine(), ComputeRequest.class);
				latency = System.currentTimeMillis() - request.getTimestamp();

				reply = handleMobileRequest(request);
				out.println(reply);
				out.flush();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					in.close();
					out.close();
					clientSock.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
		}
	}
}
