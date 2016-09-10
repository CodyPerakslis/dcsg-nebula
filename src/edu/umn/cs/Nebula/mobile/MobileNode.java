package edu.umn.cs.Nebula.mobile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
	private static final int poolSize = 10;
	private static final int requestPort = 6425;
	private static final String nebulaUrl = "http://hemant-umh.cs.umn.edu:6420/NebulaCentral/NodeHandler";
	private static final String mobileServerUrl = "http://hemant-umh.cs.umn.edu:6420/NebulaCentral/MobileHandler";
	private static final String fileDirectory = "/home/nebula/Nebula/Mobile/";
	private static final Gson gson = new Gson();

	private static ArrayList<String> neighbors = new ArrayList<String>();

	/**
	 * Make sure that the node is ready to run, i.e., it has a valid id.
	 * @throws InterruptedException
	 */
	private static boolean isReady() throws InterruptedException {
		final int interval = 2000; // in milliseconds
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

	/**
	 * A thread class that periodically send a heartbeat to the Mobile Server.
	 * The response should contain a list of neighboring nodes.
	 *  
	 * @author albert
	 */
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
						temp = gson.fromJson(response, new TypeToken<ArrayList<String>>() {}.getType());
						if (temp != null) {
							neighbors = temp;
							neighbors.remove(ip); // remove itself from the list
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

		// Connect to Nebula Central
		connect(nebulaUrl, NodeType.COMPUTE);
		if (!isReady()) return;

		// Connect to Mobile Server
		Thread mobileThread = new Thread(new MobileServerThread(mobileServerUrl));
		mobileThread.start();

		try {
			// Getting ready to accept connection
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
	 * This thread is invoked when the node receives a request. 
	 * Handle the request depending on the type of the task.
	 * 
	 * @author albert
	 */
	public static class RequestHandler implements Runnable {
		private final Socket clientSock;
		private final int bufferSize = 1024 * 1024;
		private ComputeRequest request;
		private ComputeRequest reply;

		private RequestHandler(Socket sock) {
			clientSock = sock;
		}

		/**
		 * Download a file from the client's socket.
		 * 
		 * @param path
		 * @param size
		 * @return
		 */
		private boolean downloadFile(String filename, int size) {
			InputStream in = null;
			FileOutputStream out = null;
			byte[] buffer = new byte[bufferSize];
			int bytesRead;
			boolean success = false;
			String path;

			if (filename == null || filename.isEmpty() || size < 0)
				return false;

			path = fileDirectory + filename;
			File file = new File(path);
			try {
				if (!file.exists()) file.createNewFile();
				out = new FileOutputStream(file, false);
				in = clientSock.getInputStream();
			} catch (IOException e) {
				System.out.println("[M-" + id + "] Failed downloading file (" + path + "): " + e.getMessage());
				return false;
			}

			// Read data from the stream and write it to the file
			System.out.println("[M-" + id + "] Downloading file <" + path + ", " + size + ">");
			int totalRead = 0;
			try {
				while (totalRead < size && (bytesRead = in.read(buffer)) > 0) {
					totalRead += bytesRead;
					// System.out.println("[M-" + id + "] Download: " + totalRead + "/" + size);
					out.write(buffer, 0, bytesRead);
				}
				out.flush();
				success = true;
			} catch (IOException e) {
				System.out.println("[M-" + id + "] Failed reading/writing file: " + e.getMessage());
			}

			try {
				if (in != null) in.close();
				if (out != null) out.close();
			} catch (IOException e) {
				System.out.println("[M-" + id + "] Failed closing stream: " + e.getMessage());
			}
			return success;
		}

		/**
		 * Send a file to the client's socket.
		 * 
		 * @param filename
		 * @return
		 */
		private boolean sendFile(String filename) {
			FileInputStream in;
			OutputStream out;
			byte[] buffer = new byte[bufferSize];
			int bytesWritten;
			boolean success = false;
			String path;

			if (filename == null || filename.isEmpty())
				return false;

			path = fileDirectory + filename;
			File file = new File(path);
			try {
				in = new FileInputStream(file);
				out = clientSock.getOutputStream();
			} catch (IOException e) {
				System.out.println("[M-" + id + "] Failed sending file (" + path + "): " + e.getMessage());
				return false;
			}

			// Send the file
			System.out.println("[M-" + id + "] Sending file <" + path + ", " + file.length() + ">");
			int totalWritten = 0;
			try {
				while ((bytesWritten = in.read(buffer)) > 0) {
					totalWritten += bytesWritten;
					out.write(buffer, 0, totalWritten);
					out.flush();
				}
				success = true;
			} catch (IOException e) {
				System.out.println("[M-" + id + "] Failed reading/writing file: " + e.getMessage());
			}
			
			try {
				if (in != null) in.close();
				if (out != null) out.close();
			} catch (IOException e) {
				System.out.println("[M-" + id + "] Failed closing stream: " + e.getMessage());
			}
			return success;
		}

		@Override
		public void run() {
			BufferedReader br = null;
			PrintWriter pw = null;

			try {
				br = new BufferedReader(new InputStreamReader(clientSock.getInputStream()));
				pw = new PrintWriter(clientSock.getOutputStream());
			} catch (IOException e) {
				System.out.println("[M-" + id + "] Failed opening streams from client's socket: " + e.getMessage());
				return;
			}

			try {
				request = gson.fromJson(br.readLine(), ComputeRequest.class);
			} catch (IOException e) {
				System.out.println("[M-" + id + "] Failed parsing request: " + e.getMessage());
				pw.println("Failed");
				pw.flush();
				try {
					br.close();
					pw.close();
				} catch (IOException e1) {
					e.printStackTrace();
				}
				return;
			}

			System.out.println("[M-" + id + "] Received a " + request.getRequestType() + " request");

			switch (request.getRequestType()) {
			case PING:
				reply = new ComputeRequest(ip, JobType.MOBILE, ComputeRequestType.PING);
				// include a list of nearby nodes to the reply message
				for (String peer: neighbors) {
					reply.addContent(peer);
				}
				pw.println(gson.toJson(reply));
				break;
			case UPLOAD:
				if (request.getContents() == null || request.getContents().size() == 0) {
					pw.println("Failed");
				} else {
					String filename = request.getContents().get(0);
					int filesize = Integer.parseInt(request.getContents().get(1));
					if (downloadFile(filename, filesize)) {
						System.out.println("[M-" + id + "] File upload: COMPLETE");
						pw.println("Complete");
					} else {
						System.out.println("[M-" + id + "] File upload: FAILED");
						pw.println("Failed");
					}
				}
				break;
			case DOWNLOAD:
				if (request.getContents() == null || request.getContents().size() == 0) {
					pw.println("Failed");
				} else {
					String filename = request.getContents().get(0);
					if (sendFile(filename)) {
						System.out.println("[M-" + id + "] File download: COMPLETE");
						pw.println("Complete");
					} else {
						System.out.println("[M-" + id + "] File download: FAILED");
						pw.println("Failed");
					}
				}	
				break;
			default:
				pw.println("Invalid request");
				break;
			}
			pw.flush();

			try {
				br.close();
				pw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
