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
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import edu.umn.cs.Nebula.model.JobType;
import edu.umn.cs.Nebula.model.NodeType;
import edu.umn.cs.Nebula.node.ComputeNode;
import edu.umn.cs.Nebula.request.ComputeRequest;
import edu.umn.cs.Nebula.request.ComputeRequestType;

public class MobileNode extends ComputeNode {
	private static final String nebulaUrl = "http://hemant-umh.cs.umn.edu:6420/NebulaCentral/NodeHandler";
	private static final String mobileServerUrl = "http://hemant-umh.cs.umn.edu:6420/NebulaCentral/MobileHandler";
	private static final Gson gson = new Gson();

	private static ArrayList<String> neighbors = new ArrayList<String>();
	private static HashMap<String, Process> runningApps;
	private static String fileDirectory;
	private static String appDirectory;

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
			int childStatus;

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
				System.out.println("[M-" + id + "] Neighbors: " + neighbors);
				for (String appName: runningApps.keySet()) {
					try {
						childStatus = runningApps.get(appName).exitValue();
						runningApps.remove(appName);
						System.out.println("[M-" + id + "] " + appName + " exits with exit code " + childStatus);
					} catch (IllegalThreadStateException e) {
						// the process is still running
					}
				}
				System.out.println("[M-" + id + "] Running Apps: " + runningApps.keySet());
				
				try {
					Thread.sleep(interval);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String args[]) throws InterruptedException {
		int poolSize = 10;
		int requestPort = 6425;
		runningApps = new HashMap<String, Process>();

		if (args.length < 2) {
			System.out.println("Usage: file-path, app-path");
			return;
		}
		fileDirectory = args[0];
		appDirectory = args[1];
		
		// Connect to Nebula Central
		connect(nebulaUrl, NodeType.COMPUTE);
		if (!isReady()) return;

		// Connect to Mobile Server
		Thread mobileThread = new Thread(new MobileServerThread(mobileServerUrl));
		mobileThread.start();

		waitForTasks(poolSize, requestPort);
	}

	/**
	 * Here, the compute node is performing as a client-server that listens to end users on a specific port.
	 */
	protected static void waitForTasks(int poolSize, int port) {
		ExecutorService requestPool = Executors.newFixedThreadPool(poolSize);
		ServerSocket serverSock = null;

		try {
			serverSock = new ServerSocket(port);
			System.out.println("[M-" + id + "] Listening for client requests on port " + port);
			while (true) { // listen for client requests
				requestPool.submit(new RequestHandler(serverSock.accept()));
			}
		} catch (IOException e) {
			System.err.println("[M-" + id + "] Failed listening: " + e);
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
		private boolean downloadFile(String filename, long size) {
			InputStream in = null;
			FileOutputStream out = null;
			byte[] buffer = new byte[bufferSize];
			int bytesRead;
			boolean success = false;
			String path;

			if (filename == null || filename.isEmpty() || size < 0) {
				System.out.println("[M-" + id + "] Failed downloading file <" + filename + ", " + size + ">");
				return false;
			}

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
			Process child;
			String command;
			String jobExe;
			String params;
			String appName;
			String content;

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

			// System.out.println("[M-" + id + "] Received a " + request.getRequestType() + " request");
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
				if (request.getContents() == null || request.getContents().isEmpty()) {
					System.out.println("[M-" + id + "] Failed UPLOAD request: no contents found in the request");
					pw.println("Failed");
				} else {
					String filename = request.getContents().get(0);
					long filesize = Long.parseLong(request.getContents().get(1));
					if (downloadFile(filename, filesize)) {
						System.out.println("[M-" + id + "] Downloading " + filename + ": COMPLETE");
						pw.println("Complete");
					} else {
						System.out.println("[M-" + id + "] Downloading " + filename + ": FAILED");
						pw.println("Failed");
					}
				}
				break;
			case DOWNLOAD:
				if (request.getContents() == null || request.getContents().isEmpty()) {
					System.out.println("[M-" + id + "] Failed DOWNLOAD request: no contents found in the request");
					pw.println("Failed");
				} else {
					String filename = request.getContents().get(0);
					if (sendFile(filename)) {
						System.out.println("[M-" + id + "] Uploading " + filename + ": COMPLETE");
						pw.println("Complete");
					} else {
						System.out.println("[M-" + id + "] Uploading " + filename + ": FAILED");
						pw.println("Failed");
					}
				}	
				break;
			case RUNAPP:
				if (request.getContents() == null || request.getContents().size() < 3) {
					System.out.println("[M-" + id + "] Invalid parameters to run app");
					pw.println("Failed running application");
					break;
				}
				appName = request.getContents().get(0);
				command = request.getContents().get(1);
				jobExe = request.getContents().get(2);
				params = " ";
				if (request.getContents().size() > 3) {
					params += request.getContents().get(3);
				}
				try {
					child = Runtime.getRuntime().exec(command + " " + appDirectory + jobExe + params);
				} catch (IOException e) {
					System.out.println("[M-" + id + "] Failed running application: " 
							+ command + " " + appDirectory + jobExe + params + ": " + e.getMessage());
					pw.println("Failed running application");
					break;
				}
				runningApps.put(appName, child);
				pw.println("OK");
				break;
			case STOPAPP:
				if (request.getContents() == null || request.getContents().isEmpty()) {
					System.out.println("[M-" + id + "] Invalid parameters to stop app");
					pw.println("Failed stopping application");
					break;
				}
				appName = request.getContents().get(0);
				if ((child = runningApps.get(appName)) != null) {
					child.destroy();
					runningApps.remove(appName);
					System.out.println("[M-" + id + "] " + appName + " process killed");
					pw.println("OK");
				} else {
					System.out.println("[M-" + id + "] " + appName + " is not available / no such process");
					pw.println("No such process available");
				}
				break;
			case APP_GET:
				appName = request.getContents().get(0);
				if (appName == null || request.getJobType() == null || (child = runningApps.get(appName)) == null) {
					System.out.println("[M-" + id + "] Invalid job / application");
					pw.println("Invalid job / application");
					break;
				}
				if (request.getContents().size() > 1 && request.getContents().get(1) != null) {
					content = request.getContents().get(1);
					PrintWriter childOut = new PrintWriter(child.getOutputStream());
					childOut.println(content);
					childOut.flush();
					childOut.close();
					pw.println("OK");
				} else {
					pw.println("No content available");
				}
				break;
			case APP_SEND:
				appName = request.getContents().get(0);
				if (appName == null || request.getJobType() == null || (child = runningApps.get(appName)) == null) {
					System.out.println("[M-" + id + "] Invalid job / application");
					pw.println("Invalid job / application");
					break;
				}
				if (request.getContents().size() > 1 && request.getContents().get(1) != null) {
					content = request.getContents().get(1);
					PrintWriter childOut = new PrintWriter(child.getOutputStream());
					BufferedReader childIn = new BufferedReader(new InputStreamReader(child.getInputStream()));
					childOut.println(content);
					childOut.flush();
					String response = "";
					try {
						response = childIn.readLine();
					} catch (IOException e) {
						System.out.println("[M-" + id + "] Failed reading reply from " + appName + ": " + e.getMessage());
						pw.println("Failed getting response");
						break;
					}
					pw.println(response);
					try {
						childIn.close();
						childOut.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				} else {
					pw.println("No content available");
				}
				break;
			default:
				System.out.println("[M-" + id + "] Invalid request: " + request.getRequestType());
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
