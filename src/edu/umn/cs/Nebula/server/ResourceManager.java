package edu.umn.cs.Nebula.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
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
	 * Lease request from a scheduler on one or more nodes.
	 * 
	 * @param scheduler		the scheduler that sends the leaseRequest
	 * @param leaseRequest 	a map of <nodeId, leaseTime>
	 * @return a set of node id's that are successfully leased
	 */
	private static HashMap<String, Lease> handleLease(SchedulerRequest leaseRequest) {
		HashMap<String, Lease> successfullyLeasedNodes = new HashMap<String, Lease>();

		synchronized (nodesLock) {
			for (String nodeId: leaseRequest.getLeaseNodes()) {
				// the node is not available
				if (nodes.get(nodeId) == null) {
					System.out.println("[RM] " + nodeId + " is not available."); 
					continue;
				}

				if (busyNodes.containsKey(nodeId)) {
					if (busyNodes.get(nodeId).getScheduler() != null && 
							!busyNodes.get(nodeId).getScheduler().equals(leaseRequest.getSchedulerName())) {
						// do not let a scheduler to acquire a node that has already been claimed by other scheduler
						System.out.println("[RM] " + nodeId + " is busy."); 
						continue;
					}
				}
				// create a new lease
				busyNodes.put(nodeId, leaseRequest.getLease(nodeId));
				successfullyLeasedNodes.put(nodeId, leaseRequest.getLease(nodeId));
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
	private static boolean handleRelease(SchedulerRequest leaseRequest) {
		synchronized (nodesLock) {
			for (String nodeId: leaseRequest.getNodes()) {
				if (nodes.get(nodeId) == null || busyNodes.get(nodeId) == null) {
					// ignore invalid node id
					continue;
				}
				if (busyNodes.get(nodeId).getScheduler() != null && 
						busyNodes.get(nodeId).getScheduler().equals(leaseRequest.getSchedulerName())) {
					// free the node iff it is leased by the scheduler
					busyNodes.remove(nodeId);
				}
			}
		}
		return true;
	}

	public static void main(String[] args) {
		System.out.print("[RM] Setting up resource manager ...");
		Thread nodeMonitorThread = new Thread(new MonitorThread());
		nodeMonitorThread.start();
		// Thread statusThread = new Thread(new StatusThread());
		// statusThread.start();
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
				if (nodes.isEmpty()) {
					System.out.println("[RM] No online nodes.");
				} else {
					synchronized (nodesLock) {
						for (String nodeId: nodes.keySet()) {
							if (busyNodes.containsKey(nodeId)) {
								System.out.println(nodeId + ": " + busyNodes.get(nodeId).getScheduler() + ", " + busyNodes.get(nodeId).getRemainingTime());
							} else {
								System.out.println(nodeId + ": none, 0.0");
							}
						}
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
				out = new PrintWriter(socket.getOutputStream());
				in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

				SchedulerRequest request = gson.fromJson(in.readLine(), SchedulerRequest.class);

				if (request == null) {
					System.out.println("[RM] Request not found.");
					out.println(gson.toJson(null));
				} else {
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
						break;
					case LEASE:
						HashMap<String, Lease> successfullyLeasedNodes = handleLease(request);
						System.out.println("[RM] Leased: " + successfullyLeasedNodes.keySet() + " by " + request.getSchedulerName());
						out.println(gson.toJson(successfullyLeasedNodes));
						break;
					case RELEASE:
						boolean releaseSuccess = handleRelease(request);
						out.println(gson.toJson(releaseSuccess));
						break;
					default:
						System.out.println("[RM] Invalid request: " + request.getType());
						out.println(gson.toJson(false));
					}
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
