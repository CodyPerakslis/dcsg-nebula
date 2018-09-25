package edu.umn.cs.Nebula.node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


import edu.umn.cs.Nebula.job.JobType;
import edu.umn.cs.Nebula.job.RunningTask;
import edu.umn.cs.Nebula.job.TaskInfo;
import edu.umn.cs.Nebula.job.TaskStatus;
import edu.umn.cs.Nebula.request.TaskRequest;
import edu.umn.cs.Nebula.request.TaskRequestType;



public class ComputeNode extends Node {
	/* Connection configuration */
	private static final String jobManager = "localhost"; 	// hemant:		134.84.121.87
																// local: 		131.212.250.189
																// planetlab: 	139.78.141.243
	private static final int jobManagerPort = 6422; //6412;
	private static final int jobManagerTaskUpdatePort = 6411;
	private static final int taskRequestPort = 2021;//6421;//6410;

	// Structures and locks for tasks
	private static LinkedHashMap<String, Process> runningTasks = new LinkedHashMap<String, Process>();
	private static LinkedHashMap<String, Thread> taskListeners = new LinkedHashMap<String, Thread>();
	private static LinkedHashMap<String, TaskInfo> taskStatuses = new LinkedHashMap<String, TaskInfo>();
	//not using the below hardcoding
	private static String taskDirectory = "/home/umn_nebula/albert/";
	private static final Object tasksLock = new Object();
	private static final Object taskStatusLock = new Object();
	private static final long maxInactive = 30000;
	private static String propertiespath = " ";
	private static final boolean DEBUG = true;

	/**
	 * =========================================================================
	 */

	public static void main(String args[]) throws InterruptedException, IOException {
		if (args.length > 0) {
			taskDirectory = args[0];
			propertiespath = args[1];
		}
		getNodeInformation();
		nodeInfo.setNodeType(NodeType.COMPUTE);
		
		nodeInfo.getResources().setNumCPUs(1);
		// Connect to the Job Manager
		Thread ping = new Thread(new Ping(jobManager, jobManagerPort));
		System.out.println("Pinging to jobManager:"+jobManager+" Port:"+jobManagerPort);
		ping.start();

		// Periodically check the status of all running tasks
		Thread taskMonitor = new Thread(new TaskMonitor());
		taskMonitor.start();

		// Listen for tasks
		int poolSize = 10;
		listenForTasks(poolSize, taskRequestPort);
	}

	/**
	 * Listen for tasks from the Job Manager.
	 * 
	 * @throws IOException
	 */
	protected static void listenForTasks(int poolSize, int port) throws IOException {
		ExecutorService requestPool = Executors.newFixedThreadPool(poolSize);
		ServerSocket serverSocket = null;

		try {
			serverSocket = new ServerSocket(port);
			if (DEBUG)
				System.out.println("[" + nodeInfo.getId() + "] Listening for client requests at port " + port);
			while (true) { // listen for client requests
				requestPool.submit(new TaskRequestHandler(serverSocket.accept()));
			}
		} catch (IOException e) {
			System.err.println("[" + nodeInfo.getId() + "] Failed listening: " + e);
		} finally {
			requestPool.shutdown();
			if (serverSocket != null)
				serverSocket.close();
		}
	}

	/**
	 * TASK REQUEST HANDLERS
	 * =========================================================================
	 */

	/**
	 * Periodically monitor the exit status of any of the running tasks. If a
	 * task is found, update the task information in the @NodeInfo such that the
	 * Job Manager will be aware of the status of the task. If the task is
	 * terminated or completed, terminate the process and its listener thread
	 * (if not yet terminated).
	 * 
	 * @author albert
	 */
	private static class TaskMonitor implements Runnable {
		private final int interval = 3000; // in milliseconds

		@Override
		public void run() {
			TaskStatus status;
			long lastUpdate;
			double load;

			Process process;
			Thread listener;
			HashSet<String> removedTasks = new HashSet<String>();
			long now;

			Socket socket = null;
			TaskRequest update;
			BufferedReader in = null;
			PrintWriter out = null;

			while (true) {
				now = System.currentTimeMillis();
				//update = new TaskRequest(new RunningTask(-1, JobType.MOBILE, null), TaskRequestType.UPDATE);
				update = new TaskRequest(new RunningTask(-1, JobType.STREAM, null), TaskRequestType.UPDATE);
				if (DEBUG) {
					synchronized (taskStatusLock) {
						System.out.println("[" + nodeInfo.getId() + "] Running Tasks: " + runningTasks.keySet());
						for (String processId : taskStatuses.keySet()) {
							System.out.println("\t" + processId + ": " + taskStatuses.get(processId).getInfo());
						}
					}
				}

				synchronized (taskStatusLock) {
					for (String processId : taskStatuses.keySet()) {
						// get the status of every task
						status = taskStatuses.get(processId).getStatus();
						lastUpdate = taskStatuses.get(processId).getUpdateTime();
						load = taskStatuses.get(processId).getLoad();

						if (status.equals(TaskStatus.COMPLETED) || status.equals(TaskStatus.ERROR)
								|| status.equals(TaskStatus.CANCELLED) || status.equals(TaskStatus.FAILED)) {
							removedTasks.add(processId);
							load = 0.0;
						}

						// add the last time the task sends an update
						if (now - lastUpdate > maxInactive) {
							// found a task that has been inactive for >
							// maxInactive
							removedTasks.add(processId);
							status = TaskStatus.FAILED;
						}
						// add the task info to the update message
						update.addTaskInfo(processId, new TaskInfo(status, lastUpdate, load));
					}
				}

				// remove all terminated tasks from the structure
				synchronized (tasksLock) {
					for (String processId : removedTasks) {
						// check if the process has not yet terminate
						if (runningTasks.containsKey(processId)) {
							process = runningTasks.remove(processId);
							process.destroy();
						}
						// check if the task's listener thread has not yet
						// terminate
						if (taskListeners.containsKey(processId)) {
							listener = taskListeners.remove(processId);
							listener.interrupt();
						}
					}
				}
				if (!taskStatuses.isEmpty()) {
					// send the task updates to the Job Manager
					try {
						socket = new Socket(jobManager, jobManagerTaskUpdatePort);
						out = new PrintWriter(socket.getOutputStream());
						in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						out.println(gson.toJson(update));
						out.flush();
					} catch (IOException e) {
						System.err.println("[" + nodeInfo.getId() + "] Task update failed: " + e);
					} finally {
						try {
							if (in != null)
								in.close();
							if (out != null)
								out.close();
							if (socket != null)
								socket.close();
						} catch (IOException e) {
							System.err.println("[" + nodeInfo.getId() + "] Failed closing streams/socket: " + e);
						}
					}
				}
				// remove all terminated tasks status from the structure
				synchronized (taskStatusLock) {
					for (String processId : removedTasks) {
						taskStatuses.remove(processId);
					}
				}
				removedTasks.clear();

				try {
					Thread.sleep(interval);
				} catch (InterruptedException e) {
					System.err.println("[" + nodeInfo.getId() + "] Task monitor interrupted");
					return;
				}
			}
		}
	}

	/**
	 * Handle task request
	 * 
	 * @author albert
	 */
	public static class TaskRequestHandler implements Runnable {
		private final Socket clientSock;

		private TaskRequestHandler(Socket sock) {
			clientSock = sock;
		}

		@Override
		public void run() {
			TaskRequest request = null;
			String reply = "Failed";
			BufferedReader br = null;
			PrintWriter pw = null;
			Process child;
			Process childcompile;
			String processId;
			Thread childMonitor;
			String parameters = "";

			try {
				br = new BufferedReader(new InputStreamReader(clientSock.getInputStream()));
				pw = new PrintWriter(clientSock.getOutputStream());
			} catch (IOException e) {
				System.err.println("[" + nodeInfo.getId() + "] Failed opening streams from client's socket: " + e);
				return;
			}

			try {
				// wait for a request
				request = gson.fromJson(br.readLine(), TaskRequest.class);
			} catch (IOException e) {
				System.err.println("[" + nodeInfo.getId() + "] Failed parsing request: " + e);
			}

			// check if the request is of type TaskRequest
			if (request == null) {
				pw.println("Invalid request");
				pw.flush();
				try {
					br.close();
					pw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				return;
			}

			//if (DEBUG)
			//	System.out.println("[" + nodeInfo.getId() + "] Received a " + request.getType() + " request");
			RunningTask task = request.getTask();
			switch (request.getType()) {
			case RUN:
				// a request for running a task. required: command, executable
				if (task == null || task.getCommand() == null || task.getCommand().isEmpty() || task.getExecutableFile() == null
						|| task.getExecutableFile().isEmpty()) {
					if (DEBUG)
						System.out.println("[" + nodeInfo.getId() + "] Failed to run a task");
					reply = "Missing required information to run a task";
					break;
				}
				// check if the request includes some parameters
				if (task.getParameters() != null && !task.getParameters().isEmpty()) {
					for (int i = 0; i < task.getParameters().size(); i++) {
						parameters += task.getParameters().get(i) + " ";
					}
				}
				
				if(task.isRemote())
				{
					String url = task.getUrlOfExecutableFile();
					String[] cmd = { "wget "+url };
		    		 Process script_exec;
					try {
						script_exec = Runtime.getRuntime().exec(cmd);
						script_exec.waitFor();
			    		  if(script_exec.exitValue() != 0){
			    		   System.out.println("Error while executing script");
			    		  }
			    		  BufferedReader stdInput = new BufferedReader(new
			    		                InputStreamReader(script_exec.getInputStream()));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					 
				}
				// the process ID is set to <jobID_taskID>
				processId = task.getJobId() + "_" + task.getId();
				try {
					System.out.println("Here!");
					// launch the task
					if (DEBUG)
						System.out.println("[" + nodeInfo.getId() + "] Running: " + task.getCommand() + " "
								+ taskDirectory + task.getExecutableFile() + " " + propertiespath +parameters);
					System.out.println("Setting off Edgent App");
					child = Runtime.getRuntime()
							.exec(task.getCommand() + " " + taskDirectory + task.getExecutableFile() + " " +propertiespath+ parameters);
//					//If the above does not work, please consider setting the classpath as in below example, when you load the Edgent_Filtered into workspace:
					
					//child = Runtime.getRuntime().exec("java -cp /Users/ayushi/Downloads/edgent-1.1.0/bin org.apache.edgent.samples.connectors.mqtt.edgentapp /Users/ayushi/Downloads/Nebula/mqtt.properties");
					BufferedReader stdInput2 = new BufferedReader(new 
						     InputStreamReader(child.getInputStream()));
					BufferedReader stdError2 = new BufferedReader(new 
						     InputStreamReader(child.getErrorStream()));
					// read the output from the command
					System.out.println("Here is the standard output of the command:\n");
					String s2 = null;
					while ((s2 = stdInput2.readLine()) != null) {
					    System.out.println(s2);
					}

					// read any errors from the attempted command
					System.out.println("Here is the standard error of the command (if any):\n");
					while ((s2 = stdError2.readLine()) != null) {
					    System.out.println(s2);
					}
					boolean childRunning = false;
					int exitValue = 0;
					try {
						exitValue = child.exitValue();
					} catch (IllegalThreadStateException e) {
						childRunning = true;
					}
					if (childRunning) {
						childMonitor = new Thread(new TaskListener(processId));
						childMonitor.start();
						synchronized (tasksLock) {
							runningTasks.put(processId, child);
							taskListeners.put(processId, childMonitor);
						}
					} else {
						System.out.println("[" + nodeInfo.getId() + "] Failed running task: " + task.getCommand() + " "
								+ taskDirectory + task.getExecutableFile() + " " + parameters
								+ ": process exits immediately with value " + exitValue);
						reply = "Failed running task. Process exit value: " + exitValue;
						break;
					}
				} catch (IOException e) {
					if (DEBUG) {
						System.out.println("[" + nodeInfo.getId() + "] Failed running task: " + task.getCommand() + " "
								+ taskDirectory + task.getExecutableFile() + " " + parameters + ": " + e.getMessage());
					}
					reply = "Failed running task";
					break;
				} 
				reply = "OK";
				break;
			case CANCEL:
				if (task == null || task.getJobId() < 0 || task.getId() < 0) {
					if (DEBUG)
						System.out.println("[" + nodeInfo.getId() + "] Failed to terminate a task");
					reply = "Invalid/missing required information to terminate a task";
					break;
				}
				processId = task.getJobId() + "_" + task.getId();
				synchronized (tasksLock) {
					// terminate the task's listener
					if ((childMonitor = taskListeners.get(processId)) != null) {
						childMonitor.interrupt();
						;
						taskListeners.remove(processId);
					}
					// terminate the task process
					if ((child = runningTasks.get(processId)) != null) {
						child.destroy();
						runningTasks.remove(processId);
						if (DEBUG)
							System.out.println("[" + nodeInfo.getId() + "] " + processId + " process killed");
						reply = "OK";
						// set its status as CANCELLED
						synchronized (taskStatusLock) {
							taskStatuses.put(processId,
									new TaskInfo(TaskStatus.CANCELLED, System.currentTimeMillis(), 0.0));
						}
					} else {
						if (DEBUG)
							System.out.println(
									"[" + nodeInfo.getId() + "] " + processId + " is not available / no such task");
						reply = "No such task";
					}
				}
				break;
			case DATA:
				// TODO hack code used for experiment. Need fixing.
				// assume the client knows the job_task id
				// send a data to a specific running task
				if (task == null || task.getType() == null || task.getJobId() < 0 || task.getId() < 0
						|| task.getParameters() == null || task.getParameters().isEmpty()) {
					if (DEBUG)
						System.out.println("[" + nodeInfo.getId() + "] Invalid task info");
					reply = "Invalid/missing task information";
					break;
				}

				Socket socket;
				String message = task.getLatitude() + "," + task.getLongitude() + "::";
				BufferedReader reader;
				PrintWriter writer;
				for (String parameter : task.getParameters()) {
					message += parameter + "::";
				}

				try {
					socket = new Socket("localhost", 6419);
					socket.setSoTimeout(2000);
					reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					writer = new PrintWriter(socket.getOutputStream());
					
					writer.println(message);
					writer.flush();
					
					reply = reader.readLine();
				} catch (IOException e) {
					e.printStackTrace();
					if (DEBUG)
						System.out.println("[" + nodeInfo.getId() + "] Failed sending data to task " + task.getJobId()
								+ "_" + task.getId() + ": " + e.getMessage());
					reply = "Failed sending data";
					break;
				}
				try {
					if (writer != null) writer.close();
					if (reader != null) reader.close();
					if (socket != null) socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

				break;
			default:
				if (DEBUG) {
					System.out.println("[" + nodeInfo.getId() + "] Invalid request: " + request.getType());
				}
				reply = "Invalid request";
				break;
			}
			pw.println(reply);
			pw.flush();
			try {
				br.close();
				pw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * A thread for each running task used to get update of the status of the
	 * task. The update is received as a message in a form of STATUS:LOAD
	 * 
	 * @author albert
	 */
	private static class TaskListener implements Runnable {
		private static String processId;

		public TaskListener(String id) {
			processId = id;
		}

		@Override
		public void run() {
			long now = System.currentTimeMillis();

			BufferedReader in = new BufferedReader(new InputStreamReader(runningTasks.get(processId).getInputStream()));
			String[] update;
			String message;

			while (true) {
				try {
					
					
					message = in.readLine();
					if (message == null || message.isEmpty()) {
						if (DEBUG)
							System.out.print("[" + nodeInfo.getId() + "] Empty message");
						continue;
					}
					update = message.split(":");
					now = System.currentTimeMillis();
					// Check if the update is valid
					if (update == null || update.length < 2) {
						if (DEBUG) {
							System.out.println("[" + nodeInfo.getId() + "] " + "Invalid update from process "
									+ processId + ": " + message);
						}
						continue;
					}
					synchronized (taskStatusLock) {
						// put the status in the task status information
						try {
							taskStatuses.put(processId,
									new TaskInfo(TaskStatus.valueOf(update[0]), now, Double.parseDouble(update[1])));
						} catch (NumberFormatException e) {
							if (DEBUG) {
								System.out
										.println("[" + nodeInfo.getId() + "] Invalid update from process " + processId);
							}
						}
					}
				} catch (IOException e) {
					return;
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
	}
}
