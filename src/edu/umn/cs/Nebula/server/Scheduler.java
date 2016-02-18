package edu.umn.cs.Nebula.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.Queue;

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
	protected final long SLEEP_TIME = 10000;
	protected final int MAX_JOB_SCHEDULING = 1;

	protected String applicationManagerServer = "localhost";
	protected int applicationManagerPort = 6421;
	protected String resourceManagerServer = "localhost";
	protected int resourceManagerPort = 6424;

	protected String name;

	protected HashMap<Integer, Application> applications = new HashMap<Integer, Application>();
	protected HashMap<String, NodeInfo> nodesStatus;
	protected HashMap<String, Lease> leasedNodes;
	protected final Object leaseLock = new Object();

	protected HashMap<Integer, Task> scheduledNodes = new HashMap<Integer, Task>();
	protected Queue<Job> jobQueue = new PriorityQueue<Job>(100, new Comparator<Job>() {
		@Override
		public int compare(Job job1, Job job2) {
			return job1.getPriority() - job2.getPriority();
		}
	});

	public Scheduler(String name) {
		this.name = name;
		nodesStatus = new HashMap<String, NodeInfo>();
		leasedNodes = new HashMap<String, Lease>();
	}
	
	/**
	 * Update the status of all online nodes.
	 * @return success
	 */
	protected boolean getNodes() {
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

			out.println(gson.toJson(request));
			out.flush();
			nodesStatus = gson.fromJson(in.readLine(), new TypeToken<HashMap<String, NodeInfo>>(){}.getType());
			success = true;
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
	
	protected void leaseNodes(HashMap<String, Lease> leases) {
		SchedulerRequest request = new SchedulerRequest(SchedulerRequestType.LEASE, name);
		Gson gson = new Gson();
		BufferedReader in = null;
		PrintWriter out = null;
		Socket socket = null;
		HashMap<String, Lease> successLeases = null;
		
		for (String nodeId: leases.keySet()) {
			request.addLease(nodeId, leases.get(nodeId));
		}

		try {
			socket = new Socket(resourceManagerServer, resourceManagerPort);
			out = new PrintWriter(socket.getOutputStream());
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

			out.println(gson.toJson(request));
			out.flush();
			successLeases = gson.fromJson(in.readLine(), new TypeToken<HashMap<String, Lease>>(){}.getType());

			synchronized(leaseLock) {
				for (String nodeId: successLeases.keySet()) {
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
	}

	protected boolean releaseResources(ArrayList<String> nodes) {
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

	protected void getInactiveJobs(ApplicationType type) {
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
			
			for (int id: applicationIds) {
				if (applications.containsKey(id))
					continue;
				
				request = new ApplicationRequest(ApplicationRequestType.GET, id, type);
				out.println(gson.toJson(request));
				out.flush();
				Application app = gson.fromJson(in.readLine(), Application.class);
				if (app == null) {
					System.out.println("[" + name + "] Failed getting application with id=" + id);
				} else {
					applications.put(id, app);
					for (Job job: app.getJobs().values()) {
						jobQueue.add(job);
					}
				}
			}
		} catch (IOException e) {
			System.out.println("[" + name + "] Failed getting jobs: " + e);
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
	
	protected boolean assignTasks(HashMap<String, Task> nodeTask) {
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
	
	protected class LeaseMonitor implements Runnable {
		protected final long monitorTime = 10000; // in milliseconds
		protected ArrayList<String> expiredNodes;
		
		private LeaseMonitor() {
			expiredNodes = new ArrayList<String>();
		}
		
		@Override
		public void run() {
			while (true) {
				synchronized (leaseLock) {
					for (String nodeId: leasedNodes.keySet()) {
						if (leasedNodes.get(nodeId).getRemainingTime() <= 0) {
							expiredNodes.add(nodeId);
						}
					}
					for (String nodeId: expiredNodes) {
						leasedNodes.remove(nodeId);
					}
				}
				
				if (releaseResources(expiredNodes)) {
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
