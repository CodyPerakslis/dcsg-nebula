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
import edu.umn.cs.Nebula.schedule.Schedule;

public class StreamingJobManager extends JobManager {

	/**
	 * ========================================================================
	 */

	public static void main(String[] args) throws IOException {
		start("STREAM", 
				ApplicationType.STREAM, 
				2021, 	// port used by nodes to communicate with the scheduler
				6420, 	// port used to listen for jobs
				6421, 	// port used to listen for tasks
				30, 	// number of worker threads per listener
				true);

		if (DEBUG) System.out.println("[" + name + "] Scheduler starts running");
		// start the scheduler
				Thread scheduler = new Thread(new Scheduler());
				scheduler.start();
			}

private static class Scheduler implements Runnable {
	private static final int schedulingInterval = 2000;

	@Override
	public void run() {
		Job jobToBeScheduled;
		int numScheduledTasks;

		while (true) {
			// TODO first handle tasks that need to be rescheduled
			// get a list of currently active nodes
			if (!updateOnlineNodes() || onlineNodes == null || onlineNodes.isEmpty()) {
				System.out.println("[" + name + "] Not nodes found.");
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				continue;
			}
			if (jobQueue.isEmpty()) {
				try { // go to sleep if there is no jobs need scheduling
					System.out.println("[" + name + "] Waiting for jobs");
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
				System.out.println("jobToBeScheduled is"+jobToBeScheduled.getCommand());
			}
			
			// schedule the tasks, get the number of tasks that have been
			// successfully scheduled
			if (DEBUG) System.out.println("[" + name + "] Scheduling job " + jobToBeScheduled.getId());
			numScheduledTasks = schedule(jobToBeScheduled);

			// if any of the tasks could not be scheduled, add the job back
			// to the queue
			if (numScheduledTasks < 0) {
				if (DEBUG) System.out.println("[" + name + "] No tasks can be scheduled");
				continue;
			}
			synchronized (schedulerLock) {
				if (numScheduledTasks > 0 && jobToBeScheduled.getNumTasks() > 0) {
					// update the next task to be scheduled
					jobToBeScheduled.increasePriority();
					jobQueue.add(jobToBeScheduled);
				}
			}
		}
	}

	/**
	 * Schedule a job. ASSUMPTION: each task consider a CPU as a resources.
	 * Maximum number of tasks that can be scheduled = number of CPUs
	 * offered by a node.
	 * 
	 * @param job
	 *            the job to be scheduled
	 * @return the number of tasks that are successfully scheduled
	 */
	private int schedule(Job job) {
		boolean success = false;
		HashMap<String, RunningTask> deployableTasks = new HashMap<String, RunningTask>();
		HashMap<String, Lease> leaseRequest = new HashMap<String, Lease>();
		HashMap<String, Lease> successLeases;

		
		System.out.println("Entered Scheduler");
		// check if we have any available nodes
		LinkedList<String> availableNodes = new LinkedList<String>();
		availableNodes.addAll(onlineNodes.keySet());
		availableNodes.removeAll(usedNodes.keySet());
		if (availableNodes.isEmpty()) {
			if (DEBUG) System.out.println("[" + name + "] No available nodes");
			return 0;
		}

		if (!job.getJobType().equals(JobType.STREAM)) {
			if (DEBUG) System.out.println("[" + name + "] Invalid job type: " + job.getJobType());
			return -1;
		}
		System.out.println("Going to Policy");
		// TODO fix the scheduling policy
		if(job.getTasks().keySet().isEmpty())
			System.out.println("TASKS ARE EMPTY");
		//for (long taskId : job.getTasks().keySet()) {
			if (availableNodes.size() <= 0) {
				System.out.println("ZERO Available");
				//break;
			}
			if(availableNodes.isEmpty())
				System.out.println("Available Nodes is Empty");
			// send the task to the node
			String nodeId1;
			do{
				System.out.println("Entering while loop");
				
				nodeId1 = availableNodes.removeFirst();
				System.out.println("NodeId's Lat "+onlineNodes.get(nodeId1).getLatitude()+" & Long = "+onlineNodes.get(nodeId1).getLongitude());
			} while( ! (onlineNodes.get(nodeId1).getLatitude()<job.getMaximumlatitude()) &&
					(onlineNodes.get(nodeId1).getLatitude()>job.getMinimumlatitude()) &&
					(onlineNodes.get(nodeId1).getLongitude()<job.getMaximumlongitude()) &&
					(onlineNodes.get(nodeId1).getLongitude()>job.getMinimumlongitude())  );
			System.out.println("NODE ID IS:"+nodeId1);
			
//			RunningTask task = new RunningTask(taskId, job.getJobType(), job.getCommand(), job.getExecutableFile(), TaskStatus.RUNNING);
			RunningTask task1 = new RunningTask(0, job.getJobType(), job.getCommand(), job.getExecutableFile(), TaskStatus.RUNNING, job.getUrlOfExecutableFile(),job.isRemote());
			task1.setJobId(job.getId());
			task1.setNodeId(nodeId1);

			deployableTasks.put(nodeId1, task1);
			System.out.println("Gonna call LEasing");
			leaseRequest.put(nodeId1, new Lease(name, Lease.MAX_LEASE_TIME));
		//}

		successLeases = leaseNodes(leaseRequest);
		if (successLeases == null || successLeases.isEmpty()) {
			if (DEBUG) System.out.println("[" + name + "] Failed leasing nodes: " + deployableTasks.keySet());
			return -1;
		}

		for (String nodeId : deployableTasks.keySet()) {
			if (!successLeases.containsKey(nodeId)) {
				deployableTasks.remove(nodeId);
			}
		}

		int numScheduledTasks = 0;
		RunningTask task;
		for (String nodeId : deployableTasks.keySet()) {
			// Deploy the task on the node
			task = deployableTasks.get(nodeId);
			success = sendTaskRequest(nodeId, nodePort, task, TaskRequestType.RUN);

			if (success) {
				numScheduledTasks++;
				// keep the job-task info in the runningJobs structure
				synchronized (schedulerLock) {
					if (!runningJobs.containsKey(job.getId())) {runningJobs.put(job.getId(), new LinkedList<Long>());
					}
					runningJobs.get(job.getId()).addLast(task.getId());
					runningTasks.put(job.getId() + "_" + task.getId(), task);
				}
			}
		}

		return numScheduledTasks;
	}//EOScheduleFunction
}//EOSchedulerClass
}//EOStreamingJobManager

	