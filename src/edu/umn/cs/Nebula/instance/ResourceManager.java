package edu.umn.cs.Nebula.instance;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.gson.Gson;

import edu.umn.cs.Nebula.node.NodeInfo;
import edu.umn.cs.Nebula.node.NodeType;
import edu.umn.cs.Nebula.request.SchedulerRequest;
import edu.umn.cs.Nebula.schedule.Lease;

/**
 * Nebula Resource Manager.
 * 
 * @author albert
 *
 */
public class ResourceManager {
	private static final int schedulerPort = 6414;
	private static final int poolSize = 10;
	private static final Gson gson = new Gson();

	private static HashMap<String, Lease> busyNodes = new HashMap<String, Lease>();
	
	private static NodeManager nodeManager;
	
	private static final boolean DEBUG = true;

	/** 
	 * INITIALIZATION 
	 * ======================================================================================================== */
	
	public static void main(String[] args) {
		int nodeManagerPort = 6422;
		int nodeManagerMaxInactive = 120000;
		int nodeManagerPoolSize = 50;
		String nodeDatabaseUsername = "nebula";
		String nodeDatabasePassword = "kvm";
		String nodeDatabaseServerName = "localhost";
		String nodeDatabaseName = "nebula";
		int nodeDatabasePort = 3306; // 3306 in hemant, 3307 in local

		if (args.length == 3) {
			nodeManagerPort = Integer.parseInt(args[0]);
			nodeManagerMaxInactive = Integer.parseInt(args[1]);
			nodeManagerPoolSize = Integer.parseInt(args[2]);
		} else if (args.length == 8) {
			nodeManagerPort = Integer.parseInt(args[0]);
			nodeManagerMaxInactive = Integer.parseInt(args[1]);
			nodeManagerPoolSize = Integer.parseInt(args[2]);
			nodeDatabaseUsername = args[3];
			nodeDatabasePassword = args[4];
			nodeDatabaseServerName = args[5];
			nodeDatabaseName = args[6];
			nodeDatabasePort = Integer.parseInt(args[7]);
		}

		nodeManager = new NodeManager(nodeManagerPort, nodeManagerMaxInactive, nodeManagerPoolSize, NodeType.COMPUTE);
//		nodeManager.connectDB(nodeDatabaseUsername, nodeDatabasePassword, nodeDatabaseServerName, nodeDatabaseName,
//				nodeDatabasePort);
		nodeManager.run();

		start();
	}

	/**
	 * Start listening to scheduler requests on port @schedulerPort.
	 */
	private static void start() {
		// listening for client requests
		ExecutorService requestPool = Executors.newFixedThreadPool(poolSize);
		ServerSocket serverSock = null;
		try {
			serverSock = new ServerSocket(schedulerPort);
			System.out.println("[RM] Listening for scheduler requests on port " + schedulerPort);
			while (true) {
//				System.out.println("Calling SchedulerHandle class");
//				SchedulerHandler hs = new SchedulerHandler(serverSock.accept());
				requestPool.submit(new SchedulerHandler(serverSock.accept()));
//				hs.run();
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
	 * SUBCLASSES
	 * ======================================================================================================== */
	
	/**
	 * Scheduler handler class which communicates with a scheduler for leasing and acquiring nodes.
	 * 
	 * @author albert
	 */
	public static class SchedulerHandler implements Runnable {
		private Socket socket;

		SchedulerHandler(Socket socket) {
			this.socket = socket;
			System.out.println("Socket with Sched @ "+socket.getPort());
		}

		public void run() {
			PrintWriter out = null;
			BufferedReader in = null;
			SchedulerRequest request;

			try {
				out = new PrintWriter(socket.getOutputStream());
				in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				System.out.println("Got req!!");
				request = gson.fromJson(in.readLine(), SchedulerRequest.class);
			} catch (IOException e) {
				//if (DEBUG) 
					System.out.println("[RM] Failed parsing scheduler request: " + e.getMessage());
				if (out != null) {
					out.println(gson.toJson(false));
					out.flush();
					out.close();
				}
				return;
			}

			if (request == null) {
				//if (DEBUG)
				System.out.println("[RM] Scheduler request not found.");
				out.println(gson.toJson(null));
			} else {
				//if (DEBUG)
					System.out.println("[RM] Receive a " + request.getType() + " request from " + request.getSchedulerName());

				switch(request.getType()) {
				case GETNODES:
					// return the status of all nodes, including the ones that are busy
					System.out.println("GETNODES Called");
					HashMap<String, NodeInfo> reply = new HashMap<String, NodeInfo>();
					NodeInfo nodeInfo;
					synchronized (nodeManager.nodesLock) {
						for (String nodeId: nodeManager.getNodes().keySet()) {
							nodeInfo = nodeManager.getNodes().get(nodeId);
							// add the remaining time to expire
							if (busyNodes.containsKey(nodeId)) {
								nodeInfo.setNote("" + busyNodes.get(nodeId).getRemainingTime());
							} else {
								nodeInfo.setNote("available");
							}
							reply.put(nodeId, nodeInfo);
						}
					}
					out.println(gson.toJson(reply));
					break;
				case LEASE:
					System.out.println("LEASING Called");
					HashMap<String, Lease> successfullyLeasedNodes = handleLease(request);
					System.out.println("[RM] Leased: " + successfullyLeasedNodes.keySet() + " by " + request.getSchedulerName());
					out.println(gson.toJson(successfullyLeasedNodes));
					break;
				case RELEASE:
					boolean releaseSuccess = handleRelease(request);
					out.println(gson.toJson(releaseSuccess));
					break;
				default:
					if (DEBUG) System.out.println("[RM] Invalid request: " + request.getType());
					out.println(gson.toJson(false));
				}
			}
			out.flush();

			try {
				if (in != null) in.close();
				if (out != null) out.close();
				socket.close();
			} catch (IOException e) {
				System.err.println("[RM] Failed closing streams/socket.");
			}
		}
	}

	/** 
	 * HANDLER METHODS
	 * ======================================================================================================== */
	
	/**
	 * Lease request from a scheduler on one or more nodes.
	 * 
	 * @param scheduler		the scheduler that sends the leaseRequest
	 * @param leaseRequest 	a map of <nodeId, leaseTime>
	 * @return a set of node id's that are successfully leased
	 */
	private static HashMap<String, Lease> handleLease(SchedulerRequest leaseRequest) {
		HashMap<String, Lease> successfullyLeasedNodes = new HashMap<String, Lease>();

		synchronized (nodeManager.nodesLock) {
			System.out.println("Inside Synchronisation for Lease");
			if(leaseRequest == null)
				System.out.println("Lease Req is null");
			else
				System.out.println("Lease Re is "+leaseRequest.toString());
			for (String nodeId: leaseRequest.getLeaseNodes()) {
				System.out.println("Inside for loop, node id is"+nodeId);
				if (nodeManager.getNodes().get(nodeId) == null) {
					// the node is not available
					System.out.println("[RM] " + nodeId + " is not available."); 
					continue;
				}

				if (busyNodes.containsKey(nodeId)) {
					if (busyNodes.get(nodeId).getScheduler() != null && 
							!busyNodes.get(nodeId).getScheduler().equals(leaseRequest.getSchedulerName())) {
						// do not let a scheduler acquire a node that has already been claimed by another scheduler
						if (DEBUG) System.out.println("[RM] " + nodeId + " is busy."); 
						continue;
					}
				}

				// TODO add more conditions/constraints such as lease time limit etc.

				// success. create a new lease
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
		synchronized (nodeManager.nodesLock) {
			for (String nodeId: leaseRequest.getNodes()) {
				if (nodeManager.getNodes().get(nodeId) == null || busyNodes.get(nodeId) == null) {
					continue;
				}
				if (busyNodes.get(nodeId).getScheduler() != null && 
						busyNodes.get(nodeId).getScheduler().equals(leaseRequest.getSchedulerName())) {
					// free the node only if it is currently owned by the scheduler
					busyNodes.remove(nodeId);
				}
			}
		}
		return true;
	}
}
