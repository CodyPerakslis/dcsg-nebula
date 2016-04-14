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
	private static Task task = null;
	private static final int poolSize = 10;
	private static final int requestPort = 2020;
	private static final String nebulaUrl = "http://hemant-umh.cs.umn.edu:6420/NebulaCentral/WebInterface";

	/**
	 * Get a task from Nebula from Nebula.
	 * 
	 * @return Task
	 */
	private static Task getTask() {
		Gson gson = new Gson();
		BufferedReader in = null;
		Task task = null;

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
			e.printStackTrace();
		}
		return task;
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
			System.out.println("[COMPUTE] Failed executing a child process: " + e);
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
	 * Here, the compute node is performing as a server that listens to clients on a specific port.
	 * 
	 * @param exe
	 */
	private static void runServerTask(String exe) {
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
			System.err.println("[COMPUTE] Failed to establish listening socket: " + e);
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
		System.out.println("[COMPUTE] id:" + id + "\t ip:" + ip);
		return true;
	}
		
	public static void main(String args[]) throws InterruptedException {
		connect(nebulaUrl, NodeType.COMPUTE);
		if (!isReady()) {
			return;
		}
		
		// Starting Task Fetcher
		while (true) {
			task = getTask();

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
					runServerTask(exe);
					break;
				default:
					System.out.println("[COMPUTE] Undefined handler for task of type: " + task.getType());
				}
			}
		}
	}
	
	/**
	 * This thread is invoked when a request arrives to the compute node.
	 * Handle the request depending on the type of the task.
	 */
	public static class NodeThread implements Runnable {
		private final Socket clientSock;
		private final JobType type;
		private static final int bufferSize = 4 * 1024;
		private static final int timeout = 10000;
		private static Gson gson = new Gson();
		
		NodeThread(Socket sock, JobType type) {
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
				System.out.println("Failed reading file: " + filename + ": " + e.getMessage());
				new PrintWriter(out).println("Failed getting file: " + filename + ": " + e.getMessage());
			}
		}

		private void handleMobileRequest(ComputeRequest request, OutputStream out) {
			PrintWriter pw = new PrintWriter(out);
			ComputeRequestType requestType = request.getRequestType();
			
			if (requestType.equals(ComputeRequestType.PING)) {
				System.out.println("[COMPUTE] Received a PING message.");
				ComputeRequest reply = new ComputeRequest(request.getNodeIp(), JobType.MOBILE, ComputeRequestType.PING);
				reply.setNodeIp(request.getNodeIp());
				reply.setTimestamp(System.currentTimeMillis());
				pw.println(gson.toJson(reply));
				pw.flush();
			} else if (requestType.equals(ComputeRequestType.GETFILE)) {
				System.out.println("[COMPUTE] Reveiced a GETFILE message.");
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
			long latency = -1;
			
			try {		
				in = new BufferedReader(new InputStreamReader(clientSock.getInputStream()));
				out = new PrintWriter(clientSock.getOutputStream());
				
				clientSock.setSoTimeout(timeout);

				ComputeRequest request;
				while (true) {
					request = gson.fromJson(in.readLine(), ComputeRequest.class);
					latency = System.currentTimeMillis() - request.getTimestamp();
					System.out.println("[COMPUTE] Latency from " + request.getMyIp() + ": " + latency + " ms.");
					
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
