package edu.umn.cs.Nebula.instance.scheduler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.gson.Gson;

import edu.umn.cs.Nebula.instance.NodeManager;
import edu.umn.cs.Nebula.job.ApplicationType;
import edu.umn.cs.Nebula.job.Job;
import edu.umn.cs.Nebula.job.RunningTask;
import edu.umn.cs.Nebula.job.TaskInfo;
import edu.umn.cs.Nebula.job.TaskStatus;
import edu.umn.cs.Nebula.node.NodeType;
import edu.umn.cs.Nebula.request.TaskRequest;
import edu.umn.cs.Nebula.request.TaskRequestType;

public class StreamScheduler extends Scheduler {

	
	

	/** ======================================================================================================== */

	public static void main(String[] args) throws IOException {
		init("STREAM", ApplicationType.STREAM, DEBUG);
		
		if (DEBUG) System.out.println("[" + name + "] Scheduler starts running");
		// start the scheduler
		scheduler = new Thread(new Scheduler());
		scheduler.start();
	}

	/** ======================================================================================================== */



	/**
	 * Scheduler for the job. Schedule every tasks of the job to nodes.
	 * 
	 * @author albert
	 */
	private static class Scheduler implements Runnable {
		private static final int schedulingInterval = 2000;

		@Override
		public void run() {
			Job jobToBeScheduled;
			int numTasks;
			int numScheduledTasks;

			while (true) {
				if (jobQueue.isEmpty()) {
					try { // go to sleep if there is no jobs need scheduling
						if (DEBUG) System.out.println("[JM] Waiting for jobs");
						synchronized (schedulerWait) {
							schedulerWait.wait();
						}
					} catch (InterruptedException e) {}
				} else {
					try {
						Thread.sleep(schedulingInterval);
					} catch (InterruptedException e) {}
				}

				synchronized (schedulerLock) {
					// make sure there is a job in the queue
					if (jobQueue.isEmpty()) continue;
					jobToBeScheduled = jobQueue.remove();
				}
				numTasks = jobToBeScheduled.getNumTasks();
				// schedule the tasks, get the number of tasks that have been successfully scheduled
				if (DEBUG) System.out.println("[JM] Scheduling job " + jobToBeScheduled.getId());
				numScheduledTasks = schedule(jobToBeScheduled);

				// if any of the tasks could not be scheduled, add the job back to the queue
				if (numScheduledTasks < 0) {
					if (DEBUG) System.out.println("[JM] Ignore invalid job");
					continue;
				}
				synchronized (schedulerLock) {
					if (DEBUG) System.out.println("[JM] Successfully schedule " + numScheduledTasks + " out of " + numTasks + " tasks");
					jobToBeScheduled.setNextTaskToBeScheduled(jobToBeScheduled.getNextTaskToBeScheduled() + numScheduledTasks);

					if (jobToBeScheduled.getNextTaskToBeScheduled() < numTasks) {
						// update the next task to be scheduled
						jobQueue.add(jobToBeScheduled);
					}
				}
			}
		}

		/**
		 * Schedule a job.
		 * ASSUMPTION: each task consider a CPU as a resources. Maximum
		 * number of tasks that can be scheduled = number of CPUs offered by a node.
		 * TODO implement a good policy in selecting nodes.
		 * 
		 * @param job	the job to be scheduled
		 * @return		the number of tasks that are successfully scheduled
		 */
		private int schedule(Job job) {
			LinkedList<String> onlineNodes = new LinkedList<String>(nodeHandler.updateOnlineNodes().keySet());
			int numScheduledTasks = 0;
			int numTasks = job.getNumTasks();
			boolean success = false;
			String nodeId;

			// check if we have online nodes
			if (nodeHandler.updateOnlineNodes().isEmpty()) {
				if (DEBUG) System.out.println("[JM] No available nodes found");
				return 0;
			}

			switch (job.getType()) {
			case MOBILE:
				// TODO fix the scheduling policy, for now schedule all the tasks to all nodes
				long taskIdx;
				for (taskIdx = job.getNextTaskToBeScheduled(); taskIdx < numTasks; taskIdx++) {
					int nodeIdx = 0;
					nodeId = onlineNodes.get(nodeIdx);
					if (DEBUG) System.out.println("[JM] Scheduling task " + taskIdx + " out of " + numTasks + " tasks");

					// select a node. skip a node if it has < 1 available CPU resources or 
					// the total loads of the node exceeds the maximum threshold to run this instance of task
					while (	nodeIdx < onlineNodes.size() && (nodeHandler.getAvailableResources(nodeId) < 1)) {
						nodeIdx++;
						nodeId = onlineNodes.get(nodeIdx);
					}

					if (nodeIdx == onlineNodes.size()) {
						// there is no more not-busy nodes
						if (DEBUG) System.out.println("[JM] All nodes have no resources left OR loads > threshold");
						return numScheduledTasks;
					}
					// send the task to the node
					nodeId = onlineNodes.get(nodeIdx);
					RunningTask task = new RunningTask(
							taskIdx, 
							job.getType(), 
							job.getCommand(),
							job.getExec(), 
							TaskStatus.RUNNING);
					task.setJobId(job.getId());
					task.setNodeId(nodeId);

					success = sendTaskRequest(nodeId, task, TaskRequestType.RUN);

					if (success) {
						// update the number of available resources
						nodeHandler.setAvailableResources(nodeId, nodeHandler.getAvailableResources(nodeId)-1);
						numScheduledTasks++;
						// keep the job-task info in the runningJobs structure
						synchronized (schedulerLock) {
							if (!runningJobs.containsKey(job.getId())) {
								runningJobs.put(job.getId(), new LinkedList<Long>());
							}
							runningJobs.get(job.getId()).addLast(taskIdx);
							runningTasks.put(job.getId() + "_" + taskIdx, task);
						}
					}
				}
				break;
			default:
				if (DEBUG) System.out.println("[JM] Undefined job type: " + job.getType());
				return -1;
			}

			return numScheduledTasks;
		}
	}
}
