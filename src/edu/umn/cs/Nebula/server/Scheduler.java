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
import edu.umn.cs.Nebula.model.Job;
import edu.umn.cs.Nebula.model.Task;
import edu.umn.cs.Nebula.node.NodeInfo;
import edu.umn.cs.Nebula.request.SchedulerRequest;
import edu.umn.cs.Nebula.request.SchedulerRequestType;


public abstract class Scheduler {
	protected static final long SLEEP_TIME = 10000;
	protected static final int MAX_JOB_SCHEDULING = 1;

	protected static String applicationManagerServer = "localhost";
	protected static int applicationManagerPort = 6421;
	protected static String resourceManagerServer = "localhost";
	protected static int resourceManagerPort = 6424;

	protected String name;
	protected static Socket socket;

	protected static ArrayList<Application> applications = new ArrayList<Application>();
	protected static HashMap<String, NodeInfo> nodesStatus;

	protected static HashMap<Integer, Task> scheduledNodes = new HashMap<Integer, Task>();
	protected static Queue<Job> jobQueue = new PriorityQueue<Job>(100, new Comparator<Job>() {
		@Override
		public int compare(Job job1, Job job2) {
			return job1.getPriority() - job2.getPriority();
		}
	});

	public Scheduler(String name) {
		this.name = name;
		nodesStatus = new HashMap<String, NodeInfo>();
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

	protected static void acquireResources() {
		// TODO acquire some nodes to the Resource Manager
	}

	protected static void releaseResources() {
		// TODO free acquired nodes
	}

	protected static void assignTasks() {
		// TODO assign a task on a specific compute node
	}

	protected static void getApplications() {
		// TODO get a list of applications from Application Manager
	}
}
