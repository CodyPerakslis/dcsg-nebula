package edu.umn.cs.Nebula.instance.jobManager;

import java.util.HashMap;

import edu.umn.cs.Nebula.job.ApplicationType;
import edu.umn.cs.Nebula.job.Job;
import edu.umn.cs.Nebula.job.Task;
import edu.umn.cs.Nebula.schedule.Lease;

public class MapReduceJobManager extends JobManager {
	private static final long sleepTime = 5000; // in milliseconds
	private static HashMap<String, Task> temporaryNodeTaskMapping = new HashMap<String, Task>();
	
	/**
	 * TODO implement the node selection / scheduling logic.
	 * Save the mapping in the @temporaryNodeTaskMapping and use it for
	 * task assignment after the nodes have been successfully leased.
	 *  
	 * @return
	 */
	private static HashMap<String, Lease> selectNodes(HashMap<Integer, Task> tasks, Job job) {
		HashMap<String, Lease> leases = new HashMap<String, Lease>();
		long leaseTime = 10000;
		int i = 0;
		
		for (Integer taskId: tasks.keySet()) {
			// Randomly select nodes and lease for 1 minute
			for (String nodeId: onlineNodes.keySet()) {
				if (!onlineNodes.get(nodeId).getNote().equals("0") ||
						temporaryNodeTaskMapping.containsKey(nodeId)) {
					continue;
				}
				Task temp = tasks.get(taskId);
				temp.setType(job.getJobType());
				temp.setExecutableFile(job.getExeFile());
				if (job.getFileList() != null && !job.getFileList().isEmpty() && i < job.getNumFiles()) {
					temp.setInputFile(job.getFileList().get(i));
					i++;
				}
				temporaryNodeTaskMapping.put(nodeId, temp);
				leases.put(nodeId, new Lease(name, leaseTime));
				break;
			}
		}
		
		return leases;
	}
	
	public static void main(String[] args) throws InterruptedException {
		init("MapReduce", ApplicationType.MAPREDUCE);
		Job job = null;
		HashMap<Integer, Task> tasksToBeScheduled = new HashMap<Integer, Task>();
		boolean leaseSucceed = false;
		Task task = null;
		
		while (true) {
			printStatus();
			// get a list of jobs of type appType
			getIncompleteJobs(appType);
			if (jobQueue.isEmpty()) {
				System.out.println("[" + name + "] No jobs in the queue.");
				Thread.sleep(sleepTime);
				continue;
			}
			
			// get a list of currently active nodes
			if (!updateOnlineNodes() || onlineNodes == null || onlineNodes.isEmpty()) {
				System.out.println("[" + name + "] Not nodes found.");
				Thread.sleep(sleepTime);
				continue;
			}
			
			// get a list of tasks that need to be scheduled
			// allow only 1 job at a time for scheduling, which is the job with highest priority
			// TODO implement delay scheduling technique
			System.out.println("[" + name + "] Selecting nodes for scheduling.");
			job = jobQueue.peek();
			for (int taskId: job.getTasks().keySet()) {
				if (runningTasks.containsKey(taskId) || tasksToBeScheduled.containsKey(taskId)) {
					continue;
				}
				tasksToBeScheduled.put(taskId, job.getTask(taskId));
			}
			
			HashMap<String, Lease> nodes = selectNodes(tasksToBeScheduled, job);
			if (nodes == null || nodes.isEmpty()) {
				System.out.println("[" + name + "] Not leasing.");
				Thread.sleep(sleepTime);
				continue;
			}
			
			leaseSucceed = leaseNodes(nodes);
			if (!leaseSucceed) {
				System.out.println("[" + name + "] Failed leasing.");
				Thread.sleep(sleepTime);
				continue;
			}
			
			for (String nodeId: temporaryNodeTaskMapping.keySet()) {
				if (leases.containsKey(nodeId)) {
					// if we have successfully lease the node, we can assign a task to it
					task = temporaryNodeTaskMapping.get(nodeId);
					usedNodes.put(nodeId, task);
				} else {
					temporaryNodeTaskMapping.remove(nodeId);
					System.out.println("[" + name + "] Failed leasing node: " + nodeId);
				}
			}
			if (assignTasksToNodes(usedNodes)) {
				System.out.println("[" + name + "] Tasks have been successfully scheduled.");
				for (Task assignedTask: temporaryNodeTaskMapping.values()) {
					tasksToBeScheduled.remove(assignedTask);
				}
				for (String nodeId: usedNodes.keySet()) {
					temporaryNodeTaskMapping.remove(nodeId);
				}
			} else {
				System.out.println("[" + name + "] Failed assigning tasks.");
				if (releaseNodes(temporaryNodeTaskMapping.keySet())) {
					System.out.println("[" + name + "] Releasing unused nodes.");
				} else {
					System.out.println("[" + name + "] Failed releasing unused nodes.");
				}
			}
			temporaryNodeTaskMapping.clear();
			
			Thread.sleep(sleepTime);
		}
		
	}

}
