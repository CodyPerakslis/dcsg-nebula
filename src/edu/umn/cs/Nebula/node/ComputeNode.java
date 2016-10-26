package edu.umn.cs.Nebula.node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.gson.Gson;

import edu.umn.cs.Nebula.model.JobType;
import edu.umn.cs.Nebula.model.NodeType;
import edu.umn.cs.Nebula.request.ComputeRequest;
import edu.umn.cs.Nebula.request.ComputeRequestType;

public class ComputeNode extends Node {
	private static final String nebulaUrl = "http://hemant-umh.cs.umn.edu:6420/NebulaCentral/NodeHandler";
	// private static final String nebulaUrl = "http://localhost:13993/NebulaCentral/NodeHandler";

	private static Gson gson = new Gson();

	public static void main(String args[]) throws InterruptedException {
		int requestPort = 6425;
		connect(nebulaUrl, NodeType.COMPUTE);
		if (!isReady()) return;

		startListeningForTasks(1, requestPort);
	}
	
	/**
	 * Make sure that the node is ready to run, i.e., it has its own id.
	 * @throws InterruptedException
	 */
	protected static boolean isReady() throws InterruptedException {
		final int maxFailure = 5;
		int counter = 0;
		while (id == null) {
			if (counter < maxFailure) {
				counter++;
				Thread.sleep(2000);
			} else {
				System.out.println("[C-" + id + "] Failed getting node id. Exiting.");
				return false;
			}
		}
		return true;
	}

	/**
	 * Here, the compute node is performing as a client-server that listens to end users on a specific port.
	 */
	protected static void startListeningForTasks(int poolSize, int port) {
		ExecutorService requestPool = Executors.newFixedThreadPool(poolSize);
		ServerSocket serverSock = null;

		try {
			serverSock = new ServerSocket(port);
			System.out.println("[C-" + id + "] Listening for client requests on port " + port);
			while (true) { // listen for client requests
				requestPool.submit(new RequestHandler(serverSock.accept()));
			}
		} catch (IOException e) {
			System.err.println("[C-" + id + "] Failed listening: " + e);
		}
	}

	/**
	 * This thread is invoked when the node receives a request.
	 * Handle the request depending on the type of the task.
	 */
	protected static class RequestHandler implements Runnable {
		private final Socket clientSock;

		private RequestHandler(Socket sock) {
			clientSock = sock;
		}

		@Override
		public void run() {
			BufferedReader in = null;
			OutputStream out = null;
			PrintWriter pw = null;

			try {
				in = new BufferedReader(new InputStreamReader(clientSock.getInputStream()));
				out = clientSock.getOutputStream();
				pw = new PrintWriter(out);

				ComputeRequest request = gson.fromJson(in.readLine(), ComputeRequest.class);
				System.out.println("[C-" + id + "] Received a " + request.getRequestType() + " request from " + clientSock.getInetAddress().toString());
				
				switch(request.getRequestType()) {
				// TODO implement other compute request handlers
				case PING:
					ComputeRequest reply = new ComputeRequest(ip, JobType.MOBILE, ComputeRequestType.PING);
					pw.println(gson.toJson(reply));
					pw.flush();
					break;
				default:
					pw.println("Invalid request!");
					pw.flush();
					break;
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					in.close();
					out.close();
					pw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
