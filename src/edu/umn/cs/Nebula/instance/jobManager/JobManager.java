package edu.umn.cs.Nebula.instance.jobManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import edu.umn.cs.Nebula.job.ApplicationType;
import edu.umn.cs.Nebula.job.Job;
import edu.umn.cs.Nebula.job.RunningTask;
import edu.umn.cs.Nebula.job.Task;
import edu.umn.cs.Nebula.job.TaskInfo;
import edu.umn.cs.Nebula.node.NodeInfo;
import edu.umn.cs.Nebula.request.JobRequest;
import edu.umn.cs.Nebula.request.SchedulerRequest;
import edu.umn.cs.Nebula.request.SchedulerRequestType;
import edu.umn.cs.Nebula.request.TaskRequest;
import edu.umn.cs.Nebula.request.TaskRequestType;
import edu.umn.cs.Nebula.schedule.Lease;

public abstract class JobManager {

	/* Scheduler configuration */
	protected static String name;
	protected static ApplicationType appType;
	protected static long SLEEP_TIME = 2000; // in milliseconds
	protected static long GRACE_PERIOD = 0; // in milliseconds
	
	/* Utilities */
	protected static boolean DEBUG = false;
	protected static Gson gson = new Gson();
	
	/* Node and lease utilities */
	protected static String	resourceManagerServer = "localhost";
	protected static int resourceManagerPort;
	protected static int nodePort = 2021;
	protected static HashMap<String, NodeInfo> onlineNodes = new HashMap<String, NodeInfo>();
	protected static HashMap<String, RunningTask> usedNodes = new HashMap<String, RunningTask>();
	protected static HashMap<String, Lease> leases = new HashMap<String, Lease>();
	protected static final Object leaseLock = new Object();

	/* Job/Task utilities */
	private static long jobId = 0;
	protected static int poolSize = 20;
	protected static int jobRequestPort;
	protected static int taskRequestPort;
	protected static HashMap<Long, LinkedList<Long>> runningJobs = new HashMap<Long, LinkedList<Long>>();
	protected static HashMap<String, RunningTask> runningTasks = new HashMap<String, RunningTask>();
	protected static HashMap<String, TaskInfo> runningTaskStatuses = new HashMap<String, TaskInfo>();
	protected static HashMap<Long, Task> rescheduleTasks = new HashMap<Long, Task>();
	protected static final Object schedulerWait = new Object();
	protected static final Object schedulerLock = new Object();

	protected static Queue<Job> jobQueue = new PriorityQueue<Job>(100,
			new Comparator<Job>() {
		@Override
		public int compare(Job job1, Job job2) {
			return job1.getPriority() - job2.getPriority();
		}
	});

	/** 
	 * UTILITY METHODS 
	 * ======================================================================================================== */

	/**
	 * Initialize a scheduler and run a lease-monitoring thread, which monitors
	 * leases. An expired node will be released if: (1) it is no longer used.
	 * (2) it is still being used but it has consumed the allocated time
	 * including the grace periods.
	 * 
	 * @param schedulerName
	 * @param type
	 */
	protected static void start(String schedulerName, ApplicationType type, int nodeListenerPort, int jobListenerPort, int taskListenerPort, boolean debug) {
		if (DEBUG) System.out.print("[" + schedulerName + "] initializing scheduler of type " + type);
		name = schedulerName;
		nodePort = nodeListenerPort;
		jobRequestPort = jobListenerPort;
		taskRequestPort = taskListenerPort;
		appType = type;
		DEBUG = debug;

		Thread jobListener = new Thread(new JobListener());
		jobListener.start();
		
		Thread taskListener = new Thread(new TaskListener());
		taskListener.start();
		
		Thread leaseMonitor = new Thread(new LeaseMonitor());
		leaseMonitor.start();

		if (DEBUG) {
			Thread debugger = new Thread(new Debugger());
			debugger.start();
		}
	}

	/**  
	 * SUBCLASSES
	 * ======================================================================================================== */

	/**
	 * This thread listens for any job requests.
	 * 
	 * @author albert
	 */
	private static class JobListener implements Runnable {

		@Override
		public void run() {
			ExecutorService requestPool = Executors.newFixedThreadPool(poolSize);
			ServerSocket serverSocket = null;

			try {
				serverSocket = new ServerSocket(jobRequestPort);
				if (DEBUG) System.out.println("[" + name + "] Listening for job requests at port " + jobRequestPort);
				while (true) {
					requestPool.submit(new JobRequestHandler(serverSocket.accept()));
				}
			} catch (IOException e) {
				System.err.println("[" + name + "] Failed to establish listening socket: " + e);
			} finally {
				requestPool.shutdown();
				if (serverSocket != null) {
					try {
						serverSocket.close();
					} catch (IOException e) {
						System.err.println("[" + name + "] Failed closing server socket: " + e);
					}
				}
			}
		}
	}
	
	/**
	 * This thread listens for any task requests.
	 * 
	 * @author albert
	 */
	private static class TaskListener implements Runnable {

		@Override
		public void run() {
			ExecutorService requestPool = Executors.newFixedThreadPool(poolSize);
			ServerSocket serverSocket = null;

			try {
				serverSocket = new ServerSocket(taskRequestPort);
				if (DEBUG) System.out.println("[" + name + "] Listening for task requests at port " + taskRequestPort);
				while (true) {
					requestPool.submit(new TaskRequestHandler(serverSocket.accept()));
				}
			} catch (IOException e) {
				System.err.println("[" + name + "] Failed to establish listening socket: " + e);
			} finally {
				requestPool.shutdown();
				if (serverSocket != null) {
					try {
						serverSocket.close();
					} catch (IOException e) {
						System.err.println("[" + name + "] Failed closing server socket: " + e);
					}
				}
			}
		}
	}

	/**
	 * Job request handler.
	 * 
	 * @author albert
	 */
	private static class JobRequestHandler implements Runnable {
		private final Socket clientSock;

		public JobRequestHandler(Socket socket) {
			clientSock = socket;
		}

		private String parseJobRequest(JobRequest jobRequest) {
			String response = "Invalid job request.";

			// make sure the job request is valid
			if (jobRequest == null || jobRequest.getType() == null) {
				return response;
			}

			if (DEBUG) System.out.println("[SCHEDULER] Received a job request of type " + jobRequest.getType());

			Job job = jobRequest.getJob();
			switch (jobRequest.getType()) {
			case SUBMIT:
				// job submission
				if (job != null) {
					synchronized (schedulerLock) {
						// update the job ID and put the job to the schedule queue
						job.setId(jobId);
						jobId++;
						jobQueue.add(job);
						response = "Job ID: " + Long.toString(jobId-1);
					}
					synchronized (schedulerWait) {
						// notify the scheduler there is a new job in the queue
						schedulerWait.notify();
					}
				}
				break;
			case CANCEL:
				// cancel all tasks belonging to this job
				boolean success = false;
				LinkedList<Long> tasks;
				RunningTask killedTask;
				String nodeId;

				if (job != null) {
					synchronized (schedulerLock) {
						if ((tasks = runningJobs.remove(job.getId())) != null) {
							for (Long taskId: tasks) {
								// get the task to be killed
								killedTask = runningTasks.remove(taskId);
								nodeId = killedTask.getNodeId();
								// send a CANCEL request to the node
								sendTaskRequest(nodeId, nodePort, killedTask, TaskRequestType.CANCEL);
							}
							success = true;
						}
						// make sure that the job is removed from the schedule queue if it has not been scheduled yet
						response = Boolean.toString(jobQueue.remove(job) || success);
					}
				}
				break;
			}
			return response;
		}

		public void run() {
			BufferedReader in = null;
			PrintWriter out = null;
			JobRequest jobRequest = null;
			String response = "Failed";

			try {
				in = new BufferedReader(new InputStreamReader (clientSock.getInputStream()));
				out = new PrintWriter(clientSock.getOutputStream(), true);
				String message = in.readLine();
				jobRequest = gson.fromJson(message, JobRequest.class);
			} catch (IOException e) {
				System.err.println("[" + name + "] Failed parsing job request: " + e.getMessage());
			}

			if (jobRequest != null) {
				response = parseJobRequest(jobRequest);
			}
			out.println(response);
			out.flush();
			out.close();
		}
	}

	/**
	 * Task request handler.
	 * 
	 * @author albert
	 */
	private static class TaskRequestHandler implements Runnable {
		private final Socket clientSock;

		public TaskRequestHandler(Socket socket) {
			clientSock = socket;
		}

		private String parseTaskRequest(TaskRequest taskRequest) {
			String response = "invalid request structure";

			// make sure the task request is valid
			if (taskRequest == null || taskRequest.getType() == null) {
				return response;
			}

			if (DEBUG) System.out.println("[" + name + "] Received a task request of type " + taskRequest.getType());

			RunningTask task = taskRequest.getTask();
			long taskId = task.getId();
			long jobId = task.getJobId();
			String nodeId = task.getNodeId();
			TaskInfo taskInfo;

			switch (taskRequest.getType()) {
			case CANCEL:
				// cancel the task
				synchronized (schedulerLock) {
					if (runningTasks.get(taskId) != null) {
						runningTasks.remove(taskId);
					}
					if (runningJobs.containsKey(jobId)) {
						runningJobs.get(jobId).remove(taskId);
						if (runningJobs.get(jobId).isEmpty()) {
							runningJobs.remove(jobId);
						}
					}
				}
				// send a CANCEL request to the node
				sendTaskRequest(nodeId, nodePort, task, TaskRequestType.CANCEL);
				response = "OK";
				break;
			case UPDATE:
				// status update for a task
				for (String processId: taskRequest.getTaskStatuses().keySet()) {
					taskInfo = taskRequest.getTaskStatuses().get(processId);
					nodeId = runningTasks.get(processId).getNodeId();
					jobId = Long.parseLong(processId.split("_")[0]);
					taskId = Long.parseLong(processId.split("_")[1]);

					switch (taskInfo.getStatus()) {
					case RUNNING:
						// monitor the load of each task
						synchronized (schedulerLock) {
							runningTaskStatuses.put(processId, taskInfo);
						}
						break;
					case ERROR:
					case FAILED:
						Task rescheduleTask;
						synchronized (leaseLock) {
							rescheduleTask = usedNodes.remove(processId);
						}
						synchronized (schedulerLock) {
							rescheduleTasks.put(taskId, rescheduleTask);
							runningTasks.remove(processId);
							runningTaskStatuses.remove(processId);
						}
						break;
					case CANCELLED:
					case COMPLETED:
						synchronized (schedulerLock) {
							if (runningJobs.get(jobId).isEmpty()) {
								runningJobs.remove(jobId);
							}
							runningTasks.remove(processId);
							runningJobs.get(jobId).remove(taskId);
							runningTaskStatuses.remove(processId);
							rescheduleTasks.remove(taskId);
						}
						synchronized (leaseLock) {
							usedNodes.remove(nodeId);
						}
						break;
					default:
						if (DEBUG) System.out.println("[" + name + "] Undefined task status in update request");
					}
				}
				response = "OK";
				break;
			default:
				response = "Invalid task request.";
			}
			return response;
		}

		public void run() {
			BufferedReader in = null;
			PrintWriter out = null;
			TaskRequest taskRequest = null;
			String response = "Failed";

			try {
				in = new BufferedReader(new InputStreamReader (clientSock.getInputStream()));
				out = new PrintWriter(clientSock.getOutputStream(), true);
				String message = in.readLine();
				taskRequest = gson.fromJson(message, TaskRequest.class);
			} catch (IOException e) {
				System.err.println("[" + name + "] Failed parsing task request: " + e.getMessage());
			}

			if (taskRequest != null) {
				response = parseTaskRequest(taskRequest);
			}
			out.println(response);
			out.flush();
			out.close();
		}
	}
	
	/**
	 * This thread periodically checks any expired leases. Release all expired
	 * lease whose time has exceeded the GRACE_PERIOD.
	 * 
	 * @author albert
	 */
	private static class LeaseMonitor implements Runnable {
		private HashSet<String> expiredNodes = new HashSet<String>();

		@Override
		public void run() {
			while (true) {
				synchronized (leaseLock) {
					for (String leasedNodeId : leases.keySet()) {
						// for each leased node, check if the lease has expired
						if (usedNodes.containsKey(leasedNodeId)) {
							if (leases.get(leasedNodeId).getRemainingTime() <= GRACE_PERIOD) {
								// the node is currently in-used but has been expired
								expiredNodes.add(leasedNodeId);
							}
						} else if (leases.get(leasedNodeId).getRemainingTime() <= 0) {
							// the node is not used and expired
							expiredNodes.add(leasedNodeId);
						}
					}
					// remove all expired nodes from the lease nodes list
					for (String nodeId : expiredNodes) {
						leases.remove(nodeId);
					}
				}
				// release all expired nodes
				if (!expiredNodes.isEmpty() && releaseNodes(expiredNodes)) {
					expiredNodes.clear();
				}

				try {
					Thread.sleep(SLEEP_TIME);
				} catch (InterruptedException e) {
					System.err.println("[" + name + "] Lease monitor thread failed to sleep");
				}
			}
		}
	}

	/**
	 * Debugger threads that print the information of every tasks
	 * 
	 * @author albert
	 */
	private static class Debugger implements Runnable {

		private static void printTaskStatuses() {
			System.out.println("========= TASK STATUSES =========");
			synchronized (schedulerLock) {
				System.out.println("[" + name + "] Number of jobs in queue: " + jobQueue.size());
				System.out.println("[" + name + "] Running tasks: " + runningTasks.keySet());
				System.out.println("[" + name + "] Reschedule tasks: " + rescheduleTasks.keySet());		
			}
			System.out.println("==========================\n");
		}

		private static void printNodeStatuses() {
			System.out.println("========= NODE STATUSES =========");
			synchronized (leaseLock) {
				System.out.println("[" + name + "] Online nodes: " + onlineNodes.keySet());
				System.out.println("[" + name + "] Leased nodes: " + leases.keySet());
			}
			System.out.println("==========================\n");
		}

		public void run() {
			while (true) {
				try {
					Thread.sleep(SLEEP_TIME);
				} catch (InterruptedException e) { 
					return; 
				}
				printNodeStatuses();
				printTaskStatuses();
			}
		}
	}

	/**  
	 * COMMUNICATION METHODS WITH THE RESOURCE MANAGER AND APPLICATION MANAGER
	 * ======================================================================================================== */

	/**
	 * Update the status of all online nodes. Find any used nodes that left the
	 * system and put their corresponding tasks to be rescheduled.
	 * 
	 * @return success
	 */
	protected static boolean updateOnlineNodes() {
		SchedulerRequest request = new SchedulerRequest(SchedulerRequestType.GETNODES, name);
		BufferedReader in = null;
		PrintWriter out = null;
		boolean success = false;
		Socket socket = null;

		try {
			// setup connection to the resource manager
			socket = new Socket(resourceManagerServer, resourceManagerPort);
			out = new PrintWriter(socket.getOutputStream());
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

			// send a GETNODES request to the resource manager
			out.println(gson.toJson(request));
			out.flush();
			// update the info of all nodes
			onlineNodes = gson.fromJson(in.readLine(),new TypeToken<HashMap<String, NodeInfo>>() {}.getType());
		} catch (IOException e) {
			System.err.println("[" + name + "] Failed getting nodes: " + e);
		} finally {
			try {
				if (in != null) in.close();
				if (out != null) out.close();
				if (socket != null) socket.close();
			} catch (IOException e) {
				System.err.println("[" + name + "] Failed closing streams/socket: " + e);
			}
		}

		if (onlineNodes != null) {
			for (String usedNodeId : usedNodes.keySet()) {
				if (!onlineNodes.containsKey(usedNodeId)) {
					// handle used nodes that become inactive
					synchronized (leaseLock) {
						leases.remove(usedNodeId);
					}
					// set the abandoned task to be rescheduled
					Task abandonedTask = usedNodes.remove(usedNodeId);
					if (abandonedTask != null) {
						runningTasks.remove(abandonedTask.getId());
						rescheduleTasks.put(abandonedTask.getId(), abandonedTask);
					}
				}
			}
			success = true;
		}
		return success;
	}

	/**
	 * Lease nodes from the Resource Manager.
	 * 
	 * @param leases
	 * @return
	 */
	protected static HashMap<String, Lease> leaseNodes(HashMap<String, Lease> leaseRequest) {
		SchedulerRequest request = new SchedulerRequest(SchedulerRequestType.LEASE, name);
		BufferedReader in = null;
		PrintWriter out = null;
		Socket socket = null;
		HashMap<String, Lease> successLeases = null;

		if (leaseRequest == null || leaseRequest.isEmpty()) {
			if (DEBUG) System.out.println("[" + name + "] Lease is null or empty.");
			return null;
		}

		for (String nodeId : leaseRequest.keySet()) {
			// we can only lease nodes that are currently not being used by another scheduler
			if (onlineNodes.containsKey(nodeId) 
					&& (onlineNodes.get(nodeId).getNote().equals("0") || leases.containsKey(nodeId))) {
				request.addLease(nodeId, leaseRequest.get(nodeId));
			}
		}

		try {
			// setup connection to the resource manager
			socket = new Socket(resourceManagerServer, resourceManagerPort);
			out = new PrintWriter(socket.getOutputStream());
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

			// send a LEASE request to the resource manager
			out.println(gson.toJson(request));
			out.flush();
			successLeases = gson.fromJson(in.readLine(), new TypeToken<HashMap<String, Lease>>() {}.getType());
		} catch (IOException e) {
			System.err.println("[" + name + "] Failed leasing nodes: " + e);
		} finally {
			try {
				if (in != null) in.close();
				if (out != null) out.close();
				if (socket != null) socket.close();
			} catch (IOException e) {
				System.err.println("[" + name + "] Failed closing streams/socket: " + e);
			}
		}

		if (successLeases == null || successLeases.isEmpty()) {
			if (DEBUG) System.out.println("[" + name + "] Failed leasing. Leases return with null, or empty leases.");
			return null;
		}

		// record the nodes that have been successfully leased
		synchronized (leaseLock) {
			for (String nodeId : successLeases.keySet()) {
				leases.put(nodeId, successLeases.get(nodeId));
			}
		}
		return successLeases;
	}

	/**
	 * Release nodes to the resource manager.
	 * 
	 * @param nodes
	 * @return
	 */
	protected static boolean releaseNodes(Set<String> nodes) {
		SchedulerRequest request = new SchedulerRequest(SchedulerRequestType.RELEASE, name, nodes);
		BufferedReader in = null;
		PrintWriter out = null;
		Socket socket = null;
		boolean success = false;

		try {
			// setup connection to the resource manager
			socket = new Socket(resourceManagerServer, resourceManagerPort);
			out = new PrintWriter(socket.getOutputStream());
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

			// send a REALEASE message to the resource manager
			out.println(gson.toJson(request));
			out.flush();
			success = gson.fromJson(in.readLine(), Boolean.class);
		} catch (IOException e) {
			System.err.println("[" + name + "] Failed releasing nodes: " + e);
		} finally {
			try {
				if (in != null) in.close();
				if (out != null) out.close();
				if (socket != null) socket.close();
			} catch (IOException e) {
				System.err.println("[" + name + "] Failed closing streams/socket: " + e);
			}
		}
		return success;
	}

//	/**
//	 * Get a list of incomplete jobs of type @ApplicationType from the application
//	 * manager
//	 * 
//	 * @param type
//	 */
//	protected static void getIncompleteJobs(ApplicationType type) {
//		ApplicationRequest request = new ApplicationRequest(ApplicationRequestType.GETINCOMPLETEAPP, type);
//		BufferedReader in = null;
//		PrintWriter out = null;
//		Socket socket = null;
//		ArrayList<Application> incompleteApplications = null;
//		HashSet<Long> removedTasks = new HashSet<Long>();
//
//		try {
//			// setup connection to the application manager
//			socket = new Socket(applicationManagerServer, applicationManagerPort);
//			out = new PrintWriter(socket.getOutputStream());
//			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//
//			// send a GETINCOMPLETEAPP message to the application manager
//			out.println(gson.toJson(request));
//			out.flush();
//
//			// get a list of applications of the scheduler's type
//			incompleteApplications = gson.fromJson(in.readLine(), new TypeToken<ArrayList<Application>>() {}.getType());
//		} catch (IOException e) {
//			System.err.println("[" + name + "] Failed getting jobs: " + e);
//		} finally {
//			try {
//				if (in != null) in.close();
//				if (out != null) out.close();
//				if (socket != null) socket.close();
//			} catch (IOException e) {
//				System.err.println("[" + name + "] Failed closing streams/socket: " + e);
//			}
//		}
//
//		// check if there is any applications that need to be scheduled
//		if (incompleteApplications == null || incompleteApplications.isEmpty()) {
//			if (DEBUG) System.out.println("[" + name + "] No application available.");
//			return;
//		}
//
//		for (Application incompleteApp : incompleteApplications) {
//			if (completedApps.contains(incompleteApp.getId())) {
//				// the application has actually been completed, ignore it
//				continue;
//			}
//
//			for (Job job : incompleteApp.getJobs().values()) {
//				if (!job.isActive() || job.isComplete()) {
//					// remove the job that has been completed or become inactive
//					if (jobQueue.contains(job))
//						jobQueue.remove(job);
//					// check if any of the tasks belonging to the completed/inactive job
//					// is still being monitored as "running"
//					for (RunningTask runningTask : runningTasks.values()) {
//						if (job.getTasks().containsKey(runningTask.getId())) {
//							removedTasks.add(runningTask.getId());
//						}
//					}
//					// check if any of the tasks belonging to the completed job
//					// is still included to be rescheduled
//					for (long taskId : rescheduleTasks.keySet()) {
//						if (job.getTasks().containsKey(taskId)) {
//							removedTasks.add(taskId);
//						}
//					}
//				} else if (job.isActive()) { // incomplete active job
//					if (!jobQueue.contains(job)) {
//						jobQueue.add(job);
//					}
//				}
//			}
//		}
//		if (!jobQueue.isEmpty()) {
//			synchronized (schedulerWait) {
//				// notify the scheduler there is a new job in the queue
//				schedulerWait.notify();
//			}
//		}
//
//		RunningTask killedTask;
//		for (long taskId : removedTasks) {
//			killedTask = runningTasks.remove(taskId);
//			rescheduleTasks.remove(taskId);
//			// send a CANCEL request to the node
//			sendTaskRequest(killedTask.getNodeId(), nodePort, killedTask, TaskRequestType.CANCEL);
//		}
//	}
//
//	/**
//	 * Assign a task to a specific node. The task will be added to the node's
//	 * queue in the Application Manager where each node is getting a task from.
//	 * 
//	 * @param nodeTask
//	 * @return
//	 */
//	protected static boolean assignTasksToNodes(HashMap<String, Task> nodeTask) {
//		ScheduleRequest request = new ScheduleRequest();
//		request.setNodeTask(nodeTask);
//		BufferedReader in = null;
//		PrintWriter out = null;
//		Socket socket = null;
//		boolean success = false;
//
//		try {
//			// setup connection to the application manager
//			socket = new Socket(applicationManagerServer, applicationManagerPort);
//			out = new PrintWriter(socket.getOutputStream());
//			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//
//			// send the message to the application manager
//			out.println(gson.toJson(request));
//			out.flush();
//			success = gson.fromJson(in.readLine(), Boolean.class);
//		} catch (IOException e) {
//			System.err.println("[" + name + "] Failed assigning tasks: "+ e);
//		} finally {
//			try {
//				if (in != null) in.close();
//				if (out != null) out.close();
//				if (socket != null) socket.close();
//			} catch (IOException e) {
//				System.err.println("[" + name + "] Failed closing streams/socket: " + e);
//			}
//		}
//		return success;
//	}

	/**
	 * Send a task request directly to a node.
	 * 
	 * @param nodeIp	the targeted node
	 * @param task		the task to run/cancel
	 * @param type		the request type (RUN/CANCEL) 
	 * @return
	 */
	protected static boolean sendTaskRequest(String nodeIp, int port, RunningTask task, TaskRequestType type) {
		TaskRequest request = new TaskRequest(task, type);
		BufferedReader in = null;
		PrintWriter out = null;
		Socket socket = new Socket();
		String response = null;
		boolean success = false;

		if (DEBUG) System.out.println("[" + name + "] Sending TaskRequest to " + nodeIp + ":" + port);
		try {
			// send the request to the node
			socket.connect(new InetSocketAddress(nodeIp, port), 1000);
			out = new PrintWriter(socket.getOutputStream());
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			out.println(gson.toJson(request));
			out.flush();
			response = in.readLine();
		} catch (IOException | SecurityException e) {
			System.err.println("[" + name + "] Failed sending TaskRequest: " + e);
		} finally {
			try {
				if (in != null) in.close();
				if (out != null) out.close();
				if (socket != null) socket.close();
			} catch (IOException e) { 
				System.err.println("[" + name + "] Failed closing streams/socket: " + e);
			}
		}

		if (response != null && response.equalsIgnoreCase("OK")) {
			success = true;
			if (DEBUG) System.out.println("[" + name + "] Start running task " + task.getId() + " @ " + nodeIp);
		} else {
			if (DEBUG) System.out.println("[" + name + "] Failed running task " + task.getId() + " @ " + nodeIp + ". Response: " + response);
		}
		return success;
	}
}
