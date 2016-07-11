package edu.umn.cs.Nebula.node;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.gson.Gson;

import edu.umn.cs.Nebula.model.JobType;
import edu.umn.cs.Nebula.model.Task;
import edu.umn.cs.Nebula.request.ComputeRequest;
import edu.umn.cs.Nebula.request.ComputeRequestType;

public class ComputeNode extends Node {
	private static final int max_failure = 5;
	private static final int interval = 2000; // in milliseconds
	private static final int poolSize = 10;
	private static final int requestPort = 2020;
	private static final String nebulaUrl = "http://hemant-umh.cs.umn.edu:6420/NebulaCentral/NodeHandler";
	
	private static Task task = null;
	private static Gson gson = new Gson();

	/**
	 * Get a task from Nebula from Nebula.
	 * 
	 * @return Task
	 */
	private static void getTask() {
		BufferedReader in = null;

		try {
			URL url = new URL(nebulaUrl);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setDoInput(true);
			conn.setDoOutput(true);

			PrintWriter out = new PrintWriter(conn.getOutputStream());
			out.write("id=" + id + "&requestType=GET");
			out.flush();
			conn.connect();

			if (conn.getResponseCode() == 200) {
				in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
				task = gson.fromJson(in.readLine(), Task.class);
				in.close();
			} else {
				System.out.println("[COMPUTE] Response code: " + conn.getResponseCode());
				System.out.println("[COMPUTE] Message: " + conn.getResponseMessage());
			}
		} catch (IOException e) {
			System.out.println("[COMPUTE] Failed connecting to Nebula: " + e);
		}
	}
	
	/**
	 * Create a child process and run the command.
	 * The parent process will block waiting for the child process to terminate.
	 * 
	 * @param command
	 * @return exit code
	 */
	private static int runProcess(String command) {
		int exitCode = -1;
		
		try {
			Process childProcess = Runtime.getRuntime().exec(command);
			exitCode = childProcess.waitFor();
		} catch (IOException | InterruptedException e) {
			System.out.println("[COMPUTE] Failed executing '" + command + "': " + e);
		}
		return exitCode;
	}
	
	/**
	 * TODO launch a child process for task execution and wait the child till complete
	 * Need to specify the correct command for running google-chrome with a correct task
	 */
	private static void runTask(String exe) {
		int status = runProcess(exe);
		System.out.println("[COMPUTE] Child process exits with status = " + status);
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
				requestPool.submit(new NodeThread(serverSock.accept(), task.getType()));
			}
		} catch (IOException e) {
			System.err.println("[COMPUTE] Failed listening: " + e);
		}
	}
	
	/**
	 * Make sure that the node is ready to run, i.e., it has its own id.
	 * 
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
		
		// Starting Task Fetcher
		while (true) {
			/*
			if (task == null)
				getTask();

			if (task == null) {
				System.out.println("[COMPUTE] No task.");
				Thread.sleep(interval);
			} else {
				String exe = task.getExecutableFile();
				switch(task.getType()) {
				case MAP:
				case REDUCE:
					runTask(exe);
					break;
				case MOBILE:
					runServerTask();
					break;
				default:
					System.out.println("[COMPUTE] Undefined handler for task of type: " + task.getType());
				}
			}
			*/
			runServerTask();
		}
	}
	
	/**
	 * This thread is invoked when the node receives a request.
	 * Handle the request depending on the type of the task.
	 */
	public static class NodeThread implements Runnable {
		private final Socket clientSock;
		private final JobType type;
		private static final int bufferSize = 4 * 1024;
		private static final int timeout = 10000;
		private long latency = -1;
		
		private NodeThread(Socket sock, JobType type) {
			clientSock = sock;
			this.type = type;
		}
		
		private void sendFile(String filename, OutputStream out) {
			byte[] buffer = new byte[bufferSize];
			FileInputStream in = null;

			try {
				in = new FileInputStream(filename);
				int readByte;
				int offset = 0;
				while ((readByte = in.read(buffer, offset, bufferSize)) > -1) {
					out.write(buffer, 0, readByte);
					offset += readByte;
				}
				in.close();
			} catch (IOException e) {
				System.out.println("[COMPUTE] Failed reading " + filename + ": " + e.getMessage());
				new PrintWriter(out).println("Failed getting " + filename + ": " + e.getMessage());
			}
		}

		private void handleMobileRequest(ComputeRequest request, OutputStream out) {
			PrintWriter pw = new PrintWriter(out);
			ComputeRequestType requestType = request.getRequestType();
			
			if (requestType.equals(ComputeRequestType.PING)) {
				System.out.println("[COMPUTE] PING from " + request.getMyIp() + " (" + latency + " ms)");
				ComputeRequest reply = new ComputeRequest(request.getNodeIp(), JobType.MOBILE, ComputeRequestType.PING);
				reply.setNodeIp(request.getNodeIp());
				reply.setTimestamp(System.currentTimeMillis());
				pw.println(gson.toJson(reply));
				pw.flush();
			} else if (requestType.equals(ComputeRequestType.GETFILE)) {
				System.out.println("[COMPUTE] GETFILE from " + request.getMyIp() + " (" + latency + " ms)");
				if (request.getContents() == null || request.getContents().isEmpty()) {
					pw.println("Undefined contents.");
					pw.flush();
					return;
				} else {
					sendFile(request.getContents().get(0), out);
				}
			} else {
				System.out.println("[COMPUTE] Undefined message type: " + requestType);
			}
		}
		
		@Override
		public void run() {
			BufferedReader in = null;
			PrintWriter out = null;
			
			try {		
				in = new BufferedReader(new InputStreamReader(clientSock.getInputStream()));
				out = new PrintWriter(clientSock.getOutputStream());
				clientSock.setSoTimeout(timeout);

				ComputeRequest request;
				while (true) {
					request = gson.fromJson(in.readLine(), ComputeRequest.class);
					latency = System.currentTimeMillis() - request.getTimestamp();
					
					if (!request.getJobType().equals(type)) {
						System.out.println("[COMPUTE] Invalid job type: " + request.getJobType());
						out.println("Invalid job type request.");
					} else {
						// run the task here
						switch (type) {
						case MOBILE:
							// TODO fix this. This is currently used for testing.
							handleMobileRequest(request, clientSock.getOutputStream());
							break;
						default:
							System.out.println("[COMPUTE] Invalid job type: " + request.getJobType());
							out.println("Invalid job type request.");
						}
					}
					out.flush();
				}
			} catch (InterruptedIOException e) {
				try {
					in.close();
					out.close();
					clientSock.close();
					System.out.println("[COMPUTE] Connection ended.");
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
