package edu.umn.cs.Nebula.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import edu.umn.cs.Nebula.model.Lease;
import edu.umn.cs.Nebula.node.NodeInfo;
import edu.umn.cs.Nebula.request.NodeRequest;
import edu.umn.cs.Nebula.request.NodeRequestType;
import edu.umn.cs.Nebula.request.SchedulerRequest;

/**
 * Nebula Resource Manager which uses a leasing mechanism.
 * 
 * @author albert
 *
 */
public class ResourceManager {	
	private static final int schedulerPort = 6424;
	private static final int poolSize = 10;

	private static HashMap<String, NodeInfo> nodes = new HashMap<String, NodeInfo>();
	private static HashMap<String, Lease> busyNodes = new HashMap<String, Lease>();
	private static final Object nodesLock = new Object();

	/**
	 * Get a list of nodes that are not acquired by any scheduler.
	 * 
	 * @return	available nodes
	 */
	public static Set<String> getAvailableNodeIds() {
		Set<String> temp = nodes.keySet();
		temp.removeAll(busyNodes.keySet());
		return temp;
	}

	/**
	 * Get the information of all online nodes, including those that are busy.
	 * Busy nodes provide an information on when each of them will be available in the future.
	 * 
	 * @return 	all nodes information <nodeId, <Node, waitingTime>>
	 */
	public static synchronized HashMap<String, HashMap<NodeInfo, Long>> handleGetNodes() {
		HashMap<String, HashMap<NodeInfo, Long>> result = new HashMap<String, HashMap<NodeInfo, Long>>();

		synchronized (nodesLock) {
			for (String nodeId: nodes.keySet()) {
				HashMap<NodeInfo, Long> nodeLeaseInfo = new HashMap<NodeInfo, Long>();

				if (busyNodes.get(nodeId) != null) {
					nodeLeaseInfo.put(nodes.get(nodeId), busyNodes.get(nodeId).getRemainingTime());
				} else {
					nodeLeaseInfo.put(nodes.get(nodeId), 0L);
				}
				result.put(nodeId, nodeLeaseInfo);
			}
		}
		return result;
	}

	/**
	 * Lease request from a scheduler on one or more nodes.
	 * 
	 * @param scheduler		the scheduler that sends the leaseRequest
	 * @param leaseRequest 	a map of <nodeId, leaseTime>
	 * @return a set of node id's that are successfully leased
	 */
	public static Set<String> handleLease(String scheduler, HashMap<String, Long> leaseRequest) {
		Set<String> successfullyLeasedNodes = new HashSet<String>();

		synchronized (nodesLock) {
			for (String nodeId: leaseRequest.keySet()) {
				// the node is not available
				if (nodes.get(nodeId) == null) {
					continue;
				}

				if (busyNodes.get(nodeId) != null) {
					if (busyNodes.get(nodeId).getScheduler() != null && !busyNodes.get(nodeId).getScheduler().equals(scheduler)) {
						// do not let a scheduler to acquire a node that's already claimed by other scheduler
						continue;
					}
				}
				// create a new lease
				Lease lease = new Lease(scheduler, leaseRequest.get(nodeId));
				busyNodes.put(nodeId, lease);
				successfullyLeasedNodes.add(nodeId);
			}
		}

		return successfullyLeasedNodes;
	}

	/**
	 * Free a set of nodes that are leased by a scheduler.
	 * 
	 * @param scheduler
	 * @param nodeIds
	 */
	public static void handleFree(String scheduler, Set<String> nodeIds) {
		synchronized (nodesLock) {
			for (String nodeId: nodeIds) {
				if (nodes.get(nodeId) == null || busyNodes.get(nodeId) == null) {
					// ignore invalid node id
					continue;
				}
				if (busyNodes.get(nodeId).getScheduler() != null && busyNodes.get(nodeId).getScheduler().equals(scheduler)) {
					// free the node iff it is leased by the scheduler
					busyNodes.remove(nodeId);
				}
			}
		}
	}

	public static void main(String[] args) {
		System.out.print("[RM] Setting up resource manager ...");
		Thread nodeMonitorThread = new Thread(new MonitorThread());
		nodeMonitorThread.start();
		Thread statusThread = new Thread(new StatusThread());
		statusThread.start();
		System.out.println("[OK]");
		
		// Listening for client requests
		ExecutorService requestPool = Executors.newFixedThreadPool(poolSize);
		ServerSocket serverSock = null;
		try {
			serverSock = new ServerSocket(schedulerPort);
			System.out.println("[RM] Listening for scheduler requests on port " + schedulerPort);
			while (true) {
				requestPool.submit(new SchedulerHandler(serverSock.accept()));
			}
		} catch (IOException e) {
			System.err.println("[RM] Failed to establish listening socket: " + e);
		} finally {
			requestPool.shutdown();
			if (serverSock != null) {
				try {
					serverSock.close();
				} catch (IOException e) {
					System.err.println("[RM] Failed to close listening socket");
				}
			}
		}
	}

	/**
	 * This thread class will periodically update the list of online storage nodes
	 * from Nebula Monitor.
	 * @author albert
	 *
	 */
	private static class MonitorThread implements Runnable {
		private static final String monitorUrl = "localhost";
		private static final int monitorPort = 6422;
		private static final long updateInterval = 10000;
		private static Socket socket;

		@Override
		public void run() {
			while (true) {
				BufferedReader in = null;
				PrintWriter out = null;
				Gson gson = new Gson(); 

				try {
					socket = new Socket(monitorUrl, monitorPort);

					in = new BufferedReader(new InputStreamReader (socket.getInputStream()));
					out = new PrintWriter(socket.getOutputStream(), true);

					// Send GET storage nodes request to NebulaMonitor
					NodeRequest request = new NodeRequest(null, NodeRequestType.COMPUTE);
					out.println(gson.toJson(request));
					out.flush();

					// Parse the list of online storage nodes
					HashMap<String, NodeInfo> onlineNodes = gson.fromJson(in.readLine(), 
							new TypeToken<HashMap<String, NodeInfo>>(){}.getType());

					if (onlineNodes != null) {
						synchronized (nodesLock) {
							nodes = onlineNodes;
						}
					}
					in.close();
					out.close();
					socket.close();
					Thread.sleep(updateInterval);
				} catch (IOException e) {
					System.out.println("[RM] Failed connecting to Nebula Monitor: " + e);
					return;
				} catch (InterruptedException e1) {
					System.out.println("[RM] Sleep interrupted: " + e1);
				}
			}
		}
	}
	
	private static class StatusThread implements Runnable {
		private static final long printInterval = 10000;
		
		@Override
		public void run() {
			while (true) {
				
				synchronized (nodesLock) {
					for (String nodeId: busyNodes.keySet()) {
						System.out.println(nodeId + ": " + busyNodes.get(nodeId).getScheduler() + ", " + busyNodes.get(nodeId).getRemainingTime());
					}
				}
				
				try {
					Thread.sleep(printInterval);
				} catch (InterruptedException e) {
					System.out.println("[RM] Sleep interrupted: " + e);
				}
			}
		}
		
	}

	/**
	 * Scheduler handler class which communicates with a scheduler for leasing and acquiring nodes.
	 * 
	 * @author albert
	 */
	public static class SchedulerHandler implements Runnable {
		private Socket socket;

		SchedulerHandler(Socket socket) {
			this.socket = socket;
		}

		public void run() {
			PrintWriter out = null;
			BufferedReader in = null;
			Gson gson = new Gson();

			try {
				out = new PrintWriter(socket.getOutputStream(), true);
				in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

				SchedulerRequest request = gson.fromJson(in, SchedulerRequest.class);

				if (request == null) {
					out.println(false);
					if (in != null) in.close();
					if (out != null) out.close();
					socket.close();
					return;
				}
				
				System.out.println("[RM] Receive a " + request.getType() + " request from " + request.getSchedulerName());
				
				switch(request.getType()) {
				case GET:
					// Return the status of all nodes, including the busy nodes
					HashMap<String, NodeInfo> activeNodes = new HashMap<String, NodeInfo>();

					synchronized (nodesLock) {
						for (String nodeId: nodes.keySet()) {
							NodeInfo info = nodes.get(nodeId);
							// Add the remaining time to expire
							if (busyNodes.containsKey(nodeId)) {
								info.setNote("" + busyNodes.get(nodeId).getRemainingTime());
							} else {
								info.setNote("0");
							}
							activeNodes.put(nodeId, info);
						}
					}
					out.println(gson.toJson(activeNodes));
				case LEASE:
					HashMap<String, Lease> successfullyLeasedNodes = new HashMap<String, Lease>();

					synchronized (nodesLock) {
						for (String nodeId: request.getNodes()) {
							// the node is not available
							if (!nodes.containsKey(nodeId)) {
								continue;
							}

							if (busyNodes.containsKey(nodeId)) {
								if (!busyNodes.get(nodeId).getScheduler().equals(request.getSchedulerName())) {
									// do not let a scheduler to acquire a node that's already claimed by other scheduler
									continue;
								}
							} 
							
							busyNodes.put(nodeId, request.getLease(nodeId));
							successfullyLeasedNodes.put(nodeId, request.getLease(nodeId));
						}
					}
					System.out.println("[RM] Leased: " + successfullyLeasedNodes.keySet() + " by " + request.getSchedulerName());
					out.println(gson.toJson(successfullyLeasedNodes));
				case RELEASE:
					Set<String> releasedNodes = request.getNodes();
					if (releasedNodes == null) {
						out.println(gson.toJson(false));
					} else {
						synchronized (nodesLock) {
							for (String nodeId: releasedNodes) {
								if (nodes.get(nodeId) == null || busyNodes.get(nodeId) == null) {
									continue; // ignore invalid node id
								}
								if (busyNodes.get(nodeId).getScheduler() != null && 
										busyNodes.get(nodeId).getScheduler().equalsIgnoreCase(request.getSchedulerName())) {
									// free the node iff it was leased by the scheduler that sent the request
									busyNodes.remove(nodeId);
								}
							}
						}
						// TODO need to make sure that the tasks have been terminated
						out.println(gson.toJson(true));
					}
				default:
					System.out.println("[RM] Invalid request: " + request.getType());
					out.println(gson.toJson(false));
				}
				out.flush();
			} catch (IOException e) {
				System.out.println("[RM] Failed processing request from a scheduler: " + e);
			} finally {
				try {
					if (in != null) in.close();
					if (out != null) out.close();
					socket.close();
				} catch (IOException e) {
					System.out.println("[RM] Failed closing stream/socket.");
				}
			}
		}
	}
}
