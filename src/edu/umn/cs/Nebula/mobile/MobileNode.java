package edu.umn.cs.Nebula.mobile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import edu.umn.cs.Nebula.model.JobType;
import edu.umn.cs.Nebula.model.NodeType;
import edu.umn.cs.Nebula.node.Node;
import edu.umn.cs.Nebula.request.ComputeRequest;
import edu.umn.cs.Nebula.request.ComputeRequestType;

public class MobileNode extends Node {
	private static final int interval = 2000; // in milliseconds
	private static final int poolSize = 10;
	private static final int requestPort = 6425;
	private static final String nebulaUrl = "http://hemant-umh.cs.umn.edu:6420/NebulaCentral/NodeHandler";
	private static final String mobileServerUrl = "http://hemant-umh.cs.umn.edu:6420/NebulaCentral/MobileHandler";
	private static final Gson gson = new Gson();

	private static ArrayList<String> neighbors = new ArrayList<String>();

	/**
	 * Make sure that the node is ready to run, i.e., it has its own id.
	 * 
	 * @throws InterruptedException
	 */
	private static boolean isReady() throws InterruptedException {
		final int maxFailure = 5;
		int counter = 0;

		while (id == null) {
			if (counter < maxFailure) {
				counter++;
				Thread.sleep(interval);
			} else {
				System.out.println("[M-" + id + "] Failed getting node id. Exits.");
				return false;
			}
		}
		return true;
	}

	private static class MobileServerThread implements Runnable {
		private final int interval = 10000; // in milliseconds
		private final String server;

		private MobileServerThread(String url) {
			this.server = url;
		}

		@Override
		public void run() {
			BufferedReader in = null;
			String uri = server + "?requestType=LOCAL&id=" + id + "&ip=" + ip + "&latitude=" + coordinate.getLatitude()
					+ "&longitude=" + coordinate.getLongitude();
			String response;
			ArrayList<String> temp;

			while (true) {
				try {
					URL url = new URL(uri);
					HttpURLConnection conn = (HttpURLConnection) url.openConnection();
					conn.setRequestMethod("GET");
					conn.setDoInput(true);
					conn.setDoOutput(true);
					conn.connect();

					if (conn.getResponseCode() == 200) {
						in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
						response = in.readLine();
						temp = gson.fromJson(response, new TypeToken<ArrayList<String>>() {
						}.getType());
						if (temp != null) {
							neighbors = temp;
							neighbors.remove(ip); // remove itself from the
													// local node list
							System.out.println("[M-" + id + "] Neighbors: " + neighbors);
						}
						in.close();
					} else {
						System.out.println("[M-" + id + "] Response code: " + conn.getResponseCode());
						System.out.println("[M-" + id + "] Message: " + conn.getResponseMessage());
					}
				} catch (IOException e) {
					System.out.println("[M-" + id + "] Failed connecting to Nebula: " + e);
					return;
				}
				try {
					Thread.sleep(interval);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String args[]) throws InterruptedException {
		ExecutorService requestPool = Executors.newFixedThreadPool(poolSize);
		ServerSocket serverSock = null;

		connect(nebulaUrl, NodeType.COMPUTE);
		if (!isReady())
			return;
		Thread mobileThread = new Thread(new MobileServerThread(mobileServerUrl));
		mobileThread.start();

		try {
			serverSock = new ServerSocket(requestPort);
			System.out.println("[M-" + id + "] Listening for client requests on port " + requestPort);
			while (true) { // listen for client requests
				requestPool.submit(new RequestHandler(serverSock.accept()));
			}
		} catch (IOException e) {
			System.err.println("[M-" + id + "] Exits: " + e);
			return;
		}
	}

	/**
	 * This thread is invoked when the node receives a request. Handle the
	 * request depending on the type of the task.
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
				System.out.println("[M-" + id + "] Received a " + request.getRequestType() + " request from "
						+ clientSock.getInetAddress().toString());

				switch (request.getRequestType()) {
				case PING:
					ComputeRequest reply = new ComputeRequest(ip, JobType.MOBILE, ComputeRequestType.PING);
					reply.addContent(neighbors.toString());
					pw.println(gson.toJson(reply));
					pw.flush();
					break;
				case GETFILE:
					if (request.getContents() == null || request.getContents().size() == 0) {
						pw.println("Invalid file!");
						pw.flush();
						break;
					}
					File file = new File("/home/umn_nebula/Nebula/" + request.getContents().get(0));
					if (!file.exists()) {
						pw.println("Invalid file!");
						pw.flush();
						break;
					}

					byte[] buffer = new byte[bufferSize];
					System.out.println("[M-" + id + "] Sending file: /home/nebula/Nebula/" + file.getName());
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
