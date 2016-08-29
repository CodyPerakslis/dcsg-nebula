package edu.umn.cs.Nebula.node;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
	private static final int max_failure = 5;
	private static final int interval = 2000; // in milliseconds
	private static final int poolSize = 10;
	private static final int requestPort = 6425;
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

		runServerTask();
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
			while (true) { // listen for client requests
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
		private final int bufferSize = 1572864;

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
				System.out.println("[COMPUTE] Received a " + request.getRequestType() + " request from " + clientSock.getInetAddress().toString());
				
				switch(request.getRequestType()) {
				case PING:
					ComputeRequest reply = new ComputeRequest(ip, JobType.MOBILE, ComputeRequestType.PING);
					pw.println(gson.toJson(reply));
					pw.flush();
					break;
				case GETFILE:
					if (request.getContents() == null || request.getContents().size() == 0) {
						pw.println("Invalid file!");
						pw.flush();
						break;
					}
					File file = new File("/home/nebula/Nebula/" + request.getContents().get(0));
					if (!file.exists()) {
						pw.println("Invalid file!");
						pw.flush();
						break;
					}
					
					byte[] buffer = new byte[bufferSize];
					System.out.println("[COMPUTE] Sending file: /home/nebula/Nebula/" + file.getName());
					InputStream fis = new FileInputStream(file);
			        int count;
			        long time = System.currentTimeMillis();
			        while ((count = fis.read(buffer)) > 0) {
			            out.write(buffer, 0, count);
			            out.flush();
			        }
			        time = System.currentTimeMillis() - time;
			        if (bandwidth < 0)
			        	bandwidth = 1.0 * file.length() / time;
			        else 
			        	bandwidth = (bandwidth + (1.0 * file.length() / time)) / 2;
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
