package edu.umn.cs.Nebula.node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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

public class ComputeNode extends Node {
	private static final int max_failure = 5;
	private static final int interval = 2000; // in milliseconds
	private static Task task = null;
	private static final int poolSize = 10;
	private static final int requestPort = 2020;
	private static final String nebulaUrl = "http://hemant-umh.cs.umn.edu:6420/NebulaCentral/WebInterface";

	/**
	 * Get a task from Nebula
	 * @return
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
			out.write("id=" + id + "&requestType=FETCH");
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
				requestPool.submit(new RequestThread(serverSock.accept(), task.getType(), exe));
			}
		} catch (IOException e) {
			System.err.println("[COMPUTE] Failed to establish listening socket: " + e);
		} finally {
			requestPool.shutdown();
			if (serverSock != null) {
				try {
					serverSock.close();
				} catch (IOException e) {
					System.err.println("[COMPUTE] Failed to close listening socket");
				}
			}
		}
	}
	
	public static void main(String args[]) throws InterruptedException {
		connect(nebulaUrl, NodeType.COMPUTE);
		
		int counter = 0;
		while (id == null) {
			if (counter < max_failure) {
				counter++;
				Thread.sleep(interval);
			} else {
				System.out.println("[COMPUTE] Failed getting node id. Exiting.");
				System.exit(1);
			}
		}
		System.out.println("[COMPUTE] id:" + id + "\t ip:" + ip);
		// Starting Task Fetcher
		while(true) {
			task = getTask();

			if (task == null) {
				System.out.println("[COMPUTE] No task.");
				Thread.sleep(interval);
			} else {
				String exe = task.getExecutableFile();
				System.out.println(task.getId() + " (" + task.getType() + "): " + exe);

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
	private static class RequestThread implements Runnable {
		private final Socket clientSock;
		private final JobType type;
		private final String exe;

		private RequestThread(Socket sock, JobType type, String exe) {
			clientSock = sock;
			this.type = type;
			this.exe = exe;
		}

		@Override
		public void run() {
			BufferedReader in = null;
			PrintWriter out = null;
			Gson gson = new Gson();
			
			try {
				in = new BufferedReader(new InputStreamReader(clientSock.getInputStream()));
				out = new PrintWriter(clientSock.getOutputStream());
				
				ComputeRequest request = gson.fromJson(in.readLine(), ComputeRequest.class);
				if (!request.getJobType().equals(type)) {
					// The request is of different type from the task assigned for this node
					out.println("Invalid job type request.");
				} else {
					System.out.println("[COMPUTE] Task: " + exe);
					// TODO run the task here
					// status = runProcess(exe);
				}
				out.flush();
			} catch (IOException e) {
				System.out.println("[COMPUTE] IOException in handling client request: " + e.getMessage());
				out.println("IOException in handling client request.");
			} finally {
				try {
					if (in != null)in.close();
					if (out != null) out.close();
				} catch (IOException e) {
					System.out.println("[COMPUTE] Failed closing reader/writer.");
					e.printStackTrace();
				}
			}
		}
	}
}
