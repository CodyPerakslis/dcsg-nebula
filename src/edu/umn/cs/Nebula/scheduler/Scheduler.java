package edu.umn.cs.Nebula.scheduler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import edu.umn.cs.Nebula.model.Application;
import edu.umn.cs.Nebula.model.ApplicationType;
import edu.umn.cs.Nebula.model.Job;
import edu.umn.cs.Nebula.model.Lease;
import edu.umn.cs.Nebula.model.NodeInfo;
import edu.umn.cs.Nebula.model.Task;
import edu.umn.cs.Nebula.request.ApplicationRequest;
import edu.umn.cs.Nebula.request.ApplicationRequestType;
import edu.umn.cs.Nebula.request.ScheduleRequest;
import edu.umn.cs.Nebula.request.SchedulerRequest;
import edu.umn.cs.Nebula.request.SchedulerRequestType;


public abstract class Scheduler {
	protected static final long SLEEP_TIME = 5000; // in milliseconds
	protected static final long GRACE_PERIOD = -20000; // in milliseconds
	protected static final int MAX_JOB_SCHEDULING = 1;
	protected static final Gson gson = new Gson();

	protected static String applicationManagerServer = "localhost";
	protected static int applicationManagerPort = 6421;
	protected static String resourceManagerServer = "localhost";
	protected static int resourceManagerPort = 6424;

	protected static String name;
	protected static ApplicationType appType;

	protected static HashMap<Integer, Application> applications = new HashMap<Integer, Application>();
	protected static HashMap<String, NodeInfo> nodeStatus = new HashMap<String, NodeInfo>();
	protected static HashMap<String, Lease> leasedNodes = new HashMap<String, Lease>();
	protected static final Object leaseLock = new Object();

	protected static HashMap<String, Task> scheduledNodes = new HashMap<String, Task>();
	protected static HashMap<Integer, Task> scheduledTasks = new HashMap<Integer, Task>();
	protected static HashMap<Integer, Task> rescheduleTasks = new HashMap<Integer, Task>();
	protected static HashSet<Integer> completedApps = new HashSet<Integer>();

	protected static Queue<Job> jobQueue = new PriorityQueue<Job>(100, new Comparator<Job>() {
		@Override
		public int compare(Job job1, Job job2) {
			return job1.getPriority() - job2.getPriority();
		}
	});

	/**
	 * Initialize a scheduler and run a lease-monitoring thread, which automatically release expired lease.
	 * An expired node will be released if:
	 * (1) it is not used
	 * (2) it is used but it takes longer than the grace period
	 * 
	 * @param schedulerName
	 * @param type
	 */
	protected static void init(String schedulerName, ApplicationType type) {
		System.out.print("[" + schedulerName + "] initializing scheduler of type " + type + " ...");
		name = schedulerName;
		appType = type;
		Thread leaseMonitor = new Thread(new LeaseMonitor());
		leaseMonitor.start();
		System.out.println("[OK]");
	}

	protected static void printStatus() {
		System.out.println("========= STATUS =========");
		System.out.println("[" + name + "] Online Nodes: " + nodeStatus.keySet());
		System.out.println("[" + name + "] Leased nodes: " + leasedNodes.keySet());
		System.out.println("[" + name + "] Reschedule tasks: " + rescheduleTasks.keySet());
		System.out.println("[" + name + "] Scheduled tasks: " + scheduledTasks.keySet());
		System.out.println("[" + name + "] Num Jobs in queue: " + jobQueue.size());
		System.out.println("==========================\n");
	}

	/**
	 * Update the status of all online nodes.
	 * 
	 * @return success
	 */
	protected static boolean getNodes() {
		SchedulerRequest request = new SchedulerRequest(SchedulerRequestType.GET, name);
		BufferedReader in = null;
		PrintWriter out = null;
		boolean success = false;
		Socket socket = null;

		try {
			socket = new Socket(resourceManagerServer, resourceManagerPort);
			out = new PrintWriter(socket.getOutputStream());
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			// send a GET nodes request to the resource manager
			out.println(gson.toJson(request));
			out.flush();
			// update the info of all nodes
			nodeStatus = gson.fromJson(in.readLine(), new TypeToken<HashMap<String, NodeInfo>>(){}.getType());
			if (nodeStatus != null) {
				success = true;
				for (String key: scheduledNodes.keySet()) {
					if (!nodeStatus.containsKey(key)) {
						// handle scheduled nodes that become inactive
						synchronized (leaseLock) {
							leasedNodes.remove(key);
						}
						Task abandonedTask = scheduledNodes.remove(key);
						if (abandonedTask == null) {
							continue;
						}
						scheduledTasks.remove(abandonedTask.getId());
						rescheduleTasks.put(abandonedTask.getId(), abandonedTask);
					}
				}
			} else {
				success = false;
			}
		} catch (IOException e) {
			System.out.println("[" + name + "] Failed getting nodes: " + e);
		} finally {
			try {
				if (in != null) in.close();
				if (out != null) out.close();
				if (socket != null) socket.close();
			} catch (IOException e) {
				System.out.println("[" + name + "] Failed closing streams/socket: " + e);
			}
		}
		return success;
	}

	/**
	 * Lease nodes from the Resource Manager.
	 * 
	 * @param leases
	 * @return
	 */
	protected static boolean leaseNodes(HashMap<String, Lease> leases) {
		SchedulerRequest request = new SchedulerRequest(SchedulerRequestType.LEASE, name);
		BufferedReader in = null;
		PrintWriter out = null;
		Socket socket = null;
		HashMap<String, Lease> successLeases = null;

		if (leases == null || leases.isEmpty()) {
			System.out.println("[" + name + "] Lease Map is null or empty.");
			return false;
		}

		for (String nodeId: leases.keySet()) {
			// we can only lease nodes that are currently not being used by another scheduler
			if (nodeStatus.containsKey(nodeId) && (nodeStatus.get(nodeId).getNote().equals("0") || leasedNodes.containsKey(nodeId))) {
				request.addLease(nodeId, leases.get(nodeId));
			}
		}

		try {
			socket = new Socket(resourceManagerServer, resourceManagerPort);
			out = new PrintWriter(socket.getOutputStream());
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			// send a LEASE request to the resource manager
			out.println(gson.toJson(request));
			out.flush();
			successLeases = gson.fromJson(in.readLine(), new TypeToken<HashMap<String, Lease>>(){}.getType());

			if (successLeases == null || successLeases.isEmpty()) {
				System.out.println("[" + name + "] Failed leasing. Leases return with null, or empty leases.");
				return false;
			}

			synchronized(leaseLock) {
				for (String nodeId: successLeases.keySet()) {
					// record the nodes that have been successfully leased
					leasedNodes.put(nodeId, successLeases.get(nodeId));
				}
			}
		} catch (IOException e) {
			System.out.println("[" + name + "] Failed leasing nodes: " + e);
		} finally {
			try {
				if (in != null) in.close();
				if (out != null) out.close();
				if (socket != null) socket.close();
			} catch (IOException e) {
				System.out.println("[" + name + "] Failed closing streams/socket: " + e);
			}
		}
		return true;
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
			socket = new Socket(resourceManagerServer, resourceManagerPort);
			out = new PrintWriter(socket.getOutputStream());
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			// send a REALEASE message to the resource manager
			out.println(gson.toJson(request));
			out.flush();
			success = gson.fromJson(in.readLine(), Boolean.class);
		} catch (IOException e) {
			System.out.println("[" + name + "] Failed releasing nodes: " + e);
		} finally {
			try {
				if (in != null) in.close();
				if (out != null) out.close();
				if (socket != null) socket.close();
			} catch (IOException e) {
				System.out.println("[" + name + "] Failed closing streams/socket: " + e);
			}
		}
		return success;
	}

	/**
	 * Get a list of incomplete jobs of type {@code type} from the application manager
	 * 
	 * @param type
	 */
	protected static void getIncompleteJobs(ApplicationType type) {
		ApplicationRequest request = new ApplicationRequest(ApplicationRequestType.GETINCOMPLETEAPP, type);
		BufferedReader in = null;
		PrintWriter out = null;
		Socket socket = null;
		ArrayList<Application> incompleteApplications = null;
		HashSet<Integer> removedTasks = new HashSet<Integer>();

		try {
			socket = new Socket(applicationManagerServer, applicationManagerPort);
			out = new PrintWriter(socket.getOutputStream());
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

			out.println(gson.toJson(request));
			out.flush();

			// get a list of applications of the scheduler's type
			incompleteApplications = gson.fromJson(in.readLine(), new TypeToken<ArrayList<Application>>(){}.getType());
			
			in.close();
			out.close();
			socket.close();
		} catch (IOException e) {
			System.out.println("[" + name + "] Failed getting jobs: " + e);
			e.printStackTrace();
		}	

		if (incompleteApplications == null || incompleteApplications.isEmpty()) {
			System.out.println("[" + name + "] No application available.");
			return;
		}

		for (Application incompleteApp: incompleteApplications) {
			if (completedApps.contains(incompleteApp.getId())) {
				// the application has actually been completed, ignore it
				continue;
			}

			for (Job job: incompleteApp.getJobs().values()) {
				if (!job.isActive()) { // ignore inactive jobs
					continue;
				}

				if (job.isComplete()) {
					// remove the job that has been completed
					if (jobQueue.contains(job))
						jobQueue.remove(job);
					for (int taskId: scheduledTasks.keySet()) {
						if (job.getTasks().containsKey(taskId)) {
							removedTasks.add(taskId);
						}
					}
					for (int taskId: rescheduleTasks.keySet()) {
						if (job.getTasks().containsKey(taskId)) {
							removedTasks.add(taskId);
						}
					}
				} else if (job.isActive()) {
					// incomplete active job
					if (jobQueue.contains(job)) {
						continue;
					} else {
						jobQueue.add(job);
					}
				}
			}
		}
		for (int taskId: removedTasks) {
			scheduledTasks.remove(taskId);
			rescheduleTasks.remove(taskId);
		}
	}

	/**
	 * Assign a task for a specific node. 
	 * The task will be added to the node's queue in the Application Manager
	 * where each node is getting a task from.
	 * 
	 * @param nodeTask
	 * @return
	 */
	protected static boolean assignTasks(HashMap<String, Task> nodeTask) {
		ScheduleRequest request = new ScheduleRequest();
		request.setNodeTask(nodeTask);
		BufferedReader in = null;
		PrintWriter out = null;
		Socket socket = null;
		boolean success = false;

		try {
			socket = new Socket(applicationManagerServer, applicationManagerPort);
			out = new PrintWriter(socket.getOutputStream());
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

			out.println(gson.toJson(request));
			out.flush();
			
			success = gson.fromJson(in.readLine(), Boolean.class);
			
			in.close();
			out.close();
			socket.close();
		} catch (IOException e) {
			System.out.println("[" + name + "] Failed assigning tasks: " + e);
		}
		return success;
	}

	/**
	 * This thread will periodically check expired leases.
	 * Release all expired lease whose time has exceeded the GRACE_PERIOD.
	 * 
	 * @author albert
	 */
	private static class LeaseMonitor implements Runnable {
		private final long monitorTime = 10000; // in milliseconds
		private HashSet<String> expiredNodes;

		private LeaseMonitor() {
			expiredNodes = new HashSet<String>();
		}

		@Override
		public void run() {
			while (true) {
				synchronized (leaseLock) {
					for (String nodeId: leasedNodes.keySet()) {
						// for each leased node, check if the lease has expired
						if (scheduledNodes.containsKey(nodeId)) {
							if (leasedNodes.get(nodeId).getRemainingTime() <= GRACE_PERIOD) {
								// the node is currently in-used but has expired longer than the GRACE_PERIOD
								expiredNodes.add(nodeId);
							}
						} else if (leasedNodes.get(nodeId).getRemainingTime() <= 0) {
							// the node is not used and expired
							expiredNodes.add(nodeId);
						}
					}
					for (String nodeId: expiredNodes) {
						// remove all expired nodes from the lease nodes list
						leasedNodes.remove(nodeId);
					}
				}
				// release all expired nodes
				if (!expiredNodes.isEmpty() && releaseNodes(expiredNodes)) {
					expiredNodes.clear();
				}

				try {
					Thread.sleep(monitorTime);
				} catch (InterruptedException e) {
					System.out.println("[" + name + "] Lease monitor thread failed sleeping");
				}
			}
		}
	}
}
