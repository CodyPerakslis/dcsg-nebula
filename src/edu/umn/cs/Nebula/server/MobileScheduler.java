package edu.umn.cs.Nebula.server;

import java.util.HashMap;

import edu.umn.cs.Nebula.model.ApplicationType;
import edu.umn.cs.Nebula.model.Job;
import edu.umn.cs.Nebula.model.Lease;
import edu.umn.cs.Nebula.model.Task;

public class MobileScheduler extends Scheduler {
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
		
		for (Integer taskId: tasks.keySet()) {
			// Randomly select nodes and lease for 1 minute
			for (String nodeId: nodeStatus.keySet()) {
				if (!nodeStatus.get(nodeId).getNote().equals("0") ||
						temporaryNodeTaskMapping.containsKey(nodeId)) {
					// this node is not available or has been selected for another task
					continue;
				}
				Task temp = tasks.get(taskId);
				temp.setType(job.getJobType());
				temp.setExecutableFile(job.getExeFile());
				int i = 0;
				if (job.getFileList() != null && !job.getFileList().isEmpty() && i < job.getNumFiles()) {
					// set the input file if the task require an input file
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
		init("MOBILE", ApplicationType.MOBILE);
		Job job = null;
		HashMap<Integer, Task> tasksToBeScheduled = new HashMap<Integer, Task>();
		Task task = null;
		
		while (true) {
			printStatus();
			// get a list of incomplete jobs of type appType
			getIncompleteJobs(appType);
			if (jobQueue.isEmpty()) {
				Thread.sleep(sleepTime);
				continue;
			}
			
			// get a list of currently active nodes
			if (!getNodes() || nodeStatus == null || nodeStatus.isEmpty()) {
				System.out.println("[" + name + "] No nodes found.");
				Thread.sleep(sleepTime);
				continue;
			}
			
			// get a list of tasks that need to be scheduled
			// allow only 1 job at a time for scheduling, which is the job with a highest priority
			// TODO implement delay scheduling technique
			System.out.println("[" + name + "] Selecting nodes for scheduling.");
			job = jobQueue.peek();
			for (int taskId: job.getTasks().keySet()) {
				if (!scheduledTasks.containsKey(taskId) && !tasksToBeScheduled.containsKey(taskId)) {
					// do not reschedule tasks that have been scheduled
					tasksToBeScheduled.put(taskId, job.getTask(taskId));
				}
				
			}
			// selecting nodes for tasks
			HashMap<String, Lease> nodes = selectNodes(tasksToBeScheduled, job);
			if (nodes == null || nodes.isEmpty()) {
				// either there are no available nodes or the scheduler choose not to lease any nodes
				System.out.println("[" + name + "] Not leasing.");
				Thread.sleep(sleepTime);
				continue;
			}
			
			// leasing the selected nodes
			if (!leaseNodes(nodes)) {
				System.out.println("[" + name + "] Failed leasing.");
				Thread.sleep(sleepTime);
				continue;
			}
			
			for (String nodeId: temporaryNodeTaskMapping.keySet()) {
				if (leasedNodes.containsKey(nodeId)) {
					// if we have successfully leased the node, we can assign a task to it
					task = temporaryNodeTaskMapping.get(nodeId);
					scheduledNodes.put(nodeId, task);
				} else {
					// we may not want to keep the same assignment for the subsequent iteration since 
					// the characteristic of the network and the availability of the nodes may change
					temporaryNodeTaskMapping.remove(nodeId);
					System.out.println("[" + name + "] Failed leasing node: " + nodeId);
				}
			}
			if (assignTasks(scheduledNodes)) {
				System.out.println("[" + name + "] Tasks have been successfully scheduled.");
				for (Task assignedTask: temporaryNodeTaskMapping.values()) {
					// remove tasks that have been scheduled from the list of tasks to be scheduled
					tasksToBeScheduled.remove(assignedTask);
				}
				for (String nodeId: scheduledNodes.keySet()) {
					temporaryNodeTaskMapping.remove(nodeId);
				}
			} else {
				System.out.println("[" + name + "] Failed assigning tasks.");
				if (releaseNodes(temporaryNodeTaskMapping.keySet())) {
					// release the nodes if the scheduler failed to use them
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
