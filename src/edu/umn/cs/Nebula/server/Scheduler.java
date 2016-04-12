package edu.umn.cs.Nebula.server;

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
import edu.umn.cs.Nebula.model.Task;
import edu.umn.cs.Nebula.node.NodeInfo;
import edu.umn.cs.Nebula.request.ApplicationRequest;
import edu.umn.cs.Nebula.request.ApplicationRequestType;
import edu.umn.cs.Nebula.request.ScheduleRequest;
import edu.umn.cs.Nebula.request.SchedulerRequest;
import edu.umn.cs.Nebula.request.SchedulerRequestType;


public abstract class Scheduler {
	protected static final long SLEEP_TIME = 10000; // in milliseconds
	protected static final long GRACE_PERIOD = -20000; // in milliseconds
	protected static final int MAX_JOB_SCHEDULING = 1;

	protected static String applicationManagerServer = "localhost";
	protected static int applicationManagerPort = 6421;
	protected static String resourceManagerServer = "localhost";
	protected static int resourceManagerPort = 6424;

	protected static String name;
	protected static ApplicationType appType;

	protected static HashMap<Integer, Application> applications = new HashMap<Integer, Application>();
	protected static HashMap<String, NodeInfo> nodeStatus;
	protected static HashMap<String, Lease> leasedNodes;
	protected static final Object leaseLock = new Object();

	protected static HashMap<String, Task> scheduledNodes = new HashMap<String, Task>();
	protected static HashMap<Integer, Task> scheduledTasks = new HashMap<Integer, Task>();
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
		nodeStatus = new HashMap<String, NodeInfo>();
		leasedNodes = new HashMap<String, Lease>();
		Thread leaseMonitor = new Thread(new LeaseMonitor());
		leaseMonitor.start();
		System.out.println("[OK]");
	}
	
	protected static void printStatus() {
		System.out.println("========= STATUS =========");
		System.out.println("[" + name + "] Online Nodes: " + nodeStatus.keySet());
		System.out.println("[" + name + "] Leased nodes: " + leasedNodes.keySet());
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
		Gson gson = new Gson();
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
		Gson gson = new Gson();
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
		Gson gson = new Gson();
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
		Gson gson = new Gson();
		BufferedReader in = null;
		PrintWriter out = null;
		Socket socket = null;

		try {
			socket = new Socket(applicationManagerServer, applicationManagerPort);
			out = new PrintWriter(socket.getOutputStream());
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

			out.println(gson.toJson(request));
			out.flush();
			ArrayList<Integer> applicationIds = gson.fromJson(in.readLine(), new TypeToken<ArrayList<Integer>>(){}.getType());
			if (applicationIds == null || applicationIds.isEmpty()) {
				System.out.println("[" + name + "] No application available.");
				return;
			}
			
			for (int id: applicationIds) {
				if (completedApps.contains(id)) {
					// the application has been completed, ignore it
					continue;
				}
				
				// we have seen this application before
				if (applications.containsKey(id)) {
					boolean getUpdate = false;
					for (Job job: applications.get(id).getJobs().values()) {
						// if any of its jobs is incomplete or inactive, get update on the applications
						if (!jobQueue.contains(job) || !job.isActive() || !job.isComplete()) {
							getUpdate = true;
							break;
						}
					}
					if (!getUpdate) {
						continue;
					}
				}
				
				out.close();
				in.close();
				socket.close();
				socket = new Socket(applicationManagerServer, applicationManagerPort);
				out = new PrintWriter(socket.getOutputStream());
				in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				
				request = new ApplicationRequest(ApplicationRequestType.GET, id, type);
				out.println(gson.toJson(request));
				out.flush();
				Application app = gson.fromJson(in.readLine(), Application.class);
				if (app == null) {
					System.out.println("[" + name + "] Failed getting application with id:" + id);
				} else {					
					if (applications.containsKey(id)) {
						for (Job job: app.getJobs().values()) {
							if (job.isComplete()) {
								// remove the job that has been completed
								jobQueue.remove(job);
							}
						}
						if (app.isComplete()) {
							// remove the application if it has been completed
							applications.remove(app);
							completedApps.add(app.getId());
						}
					} else if (!app.isComplete()) {
						// new application
						applications.put(id, app);
						for (Job job: app.getJobs().values()) {
							// add all of its jobs to the queue if they are not complete yet
							if (!job.isComplete()) {
								jobQueue.add(job);
							}
						}
						System.out.println("[" + name + "] NEW APP! id:" + id + ", #jobs:" + app.getJobs().size());
					} else {
						// this application is not monitored but has been completed, ignore it
						completedApps.add(app.getId());
					}
				}
			}
		} catch (IOException e) {
			System.out.println("[" + name + "] Failed getting jobs: " + e);
			e.printStackTrace();
		} finally {
			try {
				if (in != null) in.close();
				if (out != null) out.close();
				if (socket != null) socket.close();
			} catch (IOException e) {
				System.out.println("[" + name + "] Failed closing streams/socket: " + e);
			}
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
		Gson gson = new Gson();
		BufferedReader in = null;
		PrintWriter out = null;
		Socket socket = null;
		Boolean success = false;
		
		try {
			socket = new Socket(applicationManagerServer, applicationManagerPort);
			out = new PrintWriter(socket.getOutputStream());
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

			out.println(gson.toJson(request));
			out.flush();
			success = gson.fromJson(in.readLine(), Boolean.class);
		} catch (IOException e) {
			System.out.println("[" + name + "] Failed assigning tasks: " + e);
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
