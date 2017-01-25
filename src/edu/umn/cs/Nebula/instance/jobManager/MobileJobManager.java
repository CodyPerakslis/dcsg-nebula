package edu.umn.cs.Nebula.instance.jobManager;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;

import edu.umn.cs.Nebula.job.ApplicationType;
import edu.umn.cs.Nebula.job.Job;
import edu.umn.cs.Nebula.job.JobType;
import edu.umn.cs.Nebula.job.RunningTask;
import edu.umn.cs.Nebula.job.TaskStatus;
import edu.umn.cs.Nebula.request.TaskRequestType;
import edu.umn.cs.Nebula.schedule.Lease;

public class MobileJobManager extends JobManager {

	/** ======================================================================================================== */

	public static void main(String[] args) throws IOException {
		start("MOBILE", ApplicationType.STREAM, 2021, 6420, 6421, true);

		if (DEBUG) System.out.println("[" + name + "] Scheduler starts running");
		// start the scheduler
		Thread scheduler = new Thread(new Scheduler());
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
				// TODO first handle tasks that need to be rescheduled


				if (jobQueue.isEmpty()) {
					try { // go to sleep if there is no jobs need scheduling
						if (DEBUG) System.out.println("[" + name + "] Waiting for jobs");
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
				if (DEBUG) System.out.println("[" + name + "] Scheduling job " + jobToBeScheduled.getId());
				numScheduledTasks = schedule(jobToBeScheduled);

				// if any of the tasks could not be scheduled, add the job back to the queue
				if (numScheduledTasks < 0) {
					if (DEBUG) System.out.println("[" + name + "] No tasks can be scheduled");
					continue;
				}
				synchronized (schedulerLock) {
					if (DEBUG) System.out.println("[" + name + "] Successfully schedule " + numScheduledTasks + " out of " + numTasks + " tasks");

					if (numScheduledTasks > 0 && jobToBeScheduled.getNumTasks() > 0) {
						// update the next task to be scheduled
						jobToBeScheduled.increasePriority();
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
			int numTasks = job.getNumTasks();
			boolean success = false;
			HashMap<String, RunningTask> deployableTasks = new HashMap<String, RunningTask>();
			HashMap<String, Lease> leaseRequest = new HashMap<String, Lease>();
			HashMap<String, Lease> successLeases;

			// check if we have any available nodes
			LinkedList<String> availableNodes = new LinkedList<String>();
			availableNodes.addAll(onlineNodes.keySet());
			availableNodes.removeAll(usedNodes.keySet());
			if (availableNodes.isEmpty()) {
				if (DEBUG) System.out.println("[" + name + "] No available nodes found");
				return 0;
			}

			if (!job.getJobType().equals(JobType.MOBILE)) {
				if (DEBUG) System.out.println("[" + name + "] Invalid job type: " + job.getJobType());
				return -1;
			}
			
			// TODO fix the scheduling policy using the grid index
			for (long taskId: job.getTasks().keySet()) {
				if (availableNodes.size() <= 0) {
					break;
				}
				if (DEBUG) System.out.println("[" + name + "] Scheduling task " + taskId + " out of " + numTasks + " tasks");

				// send the task to the node
				String nodeId = availableNodes.removeFirst();
				RunningTask task = new RunningTask(
						taskId, 
						job.getJobType(), 
						job.getCommand(),
						job.getExecutableFile(),
						TaskStatus.RUNNING);
				task.setJobId(job.getId());
				task.setNodeId(nodeId);

				deployableTasks.put(nodeId, task);
				// TODO fix the lease
				leaseRequest.put(nodeId, new Lease(name, Lease.MAX_LEASE_TIME)); 
			}

			successLeases = leaseNodes(leaseRequest);
			if (successLeases == null || successLeases.isEmpty()) {
				if (DEBUG) System.out.println("[" + name + "] Failed leasing nodes: " + deployableTasks.keySet());
				return -1;
			}
			
			for (String nodeId: deployableTasks.keySet()) {
				if (!successLeases.containsKey(nodeId)) {
					deployableTasks.remove(nodeId);
				}
			}
			
			int numScheduledTasks = 0;
			RunningTask task;
			for (String nodeId: deployableTasks.keySet()) {
				// Deploy the task on the node
				task = deployableTasks.get(nodeId);
				success = sendTaskRequest(nodeId, nodePort, task, TaskRequestType.RUN);

				if (success) {				
					numScheduledTasks++;
					// keep the job-task info in the runningJobs structure
					synchronized (schedulerLock) {
						if (!runningJobs.containsKey(job.getId())) {
							runningJobs.put(job.getId(), new LinkedList<Long>());
						}
						runningJobs.get(job.getId()).addLast(task.getId());
						runningTasks.put(job.getId() + "_" + task.getId(), task);
					}
				}
			}

			return numScheduledTasks;
		}
	}
}
