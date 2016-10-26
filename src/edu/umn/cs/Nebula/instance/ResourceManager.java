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

import edu.umn.cs.Nebula.model.Lease;
import edu.umn.cs.Nebula.model.NodeInfo;
import edu.umn.cs.Nebula.model.NodeType;
import edu.umn.cs.Nebula.request.SchedulerRequest;

/**
 * Nebula Resource Manager.
 * 
 * @author albert
 *
 */
public class ResourceManager {
	private static final int schedulerPort = 6426;
	private static final int poolSize = 10;
	private static final Gson gson = new Gson();

	private static HashMap<String, Lease> busyNodes = new HashMap<String, Lease>();
	private static NodeHandler nodeHandler;

	public static void main(String[] args) {
		if (args.length < 4) {
			nodeHandler = new NodeHandler(6422, NodeType.COMPUTE, 5000, 50);
			nodeHandler.connectDB("nebula", "kvm", "localhost", "nebula", 3307); // set to 3306 in hemant, 3307 in local
		} else if (args.length == 4){
			nodeHandler = new NodeHandler(Integer.parseInt(args[0]), NodeType.valueOf(args[1]),
					Integer.parseInt(args[2]), Integer.parseInt(args[3]));
		} else if (args.length == 9) {
			nodeHandler = new NodeHandler(Integer.parseInt(args[0]), NodeType.valueOf(args[1]),
					Integer.parseInt(args[2]), Integer.parseInt(args[3]));
			nodeHandler.connectDB(args[4], args[5], args[6], args[7], Integer.parseInt(args[8]));
		}
		nodeHandler.start();

		start();
	}

	private static void start() {
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
			SchedulerRequest request;

			try {
				out = new PrintWriter(socket.getOutputStream());
				in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

				request = gson.fromJson(in.readLine(), SchedulerRequest.class);
			} catch (IOException e) {
				System.out.println("[RM] Failed parsing scheduler request: " + e.getMessage());
				if (out != null) {
					out.println(gson.toJson(false));
					out.flush();
					out.close();
				}
				return;
			}

			if (request == null) {
				System.out.println("[RM] Scheduler request not found.");
				out.println(gson.toJson(null));
			} else {
				System.out.println("[RM] Receive a " + request.getType() + " request from " + request.getSchedulerName());

				switch(request.getType()) {
				case GET:
					// return the status of all nodes, including the ones that are busy
					HashMap<String, NodeInfo> reply = new HashMap<String, NodeInfo>();
					NodeInfo nodeInfo;
					synchronized (nodeHandler.nodesLock) {
						for (String nodeId: nodeHandler.nodes.keySet()) {
							nodeInfo = nodeHandler.nodes.get(nodeId);
							// Add the remaining time to expire
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

			try {
				if (in != null) in.close();
				if (out != null) out.close();
				socket.close();
			} catch (IOException e) {
				System.out.println("[RM] Failed closing streams/socket.");
			}
		}
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

		synchronized (nodeHandler.nodesLock) {
			for (String nodeId: leaseRequest.getLeaseNodes()) {
				if (nodeHandler.nodes.get(nodeId) == null) {
					// The node is not available
					System.out.println("[RM] " + nodeId + " is not available."); 
					continue;
				}

				if (busyNodes.containsKey(nodeId)) {
					if (busyNodes.get(nodeId).getScheduler() != null && 
							!busyNodes.get(nodeId).getScheduler().equals(leaseRequest.getSchedulerName())) {
						// Do not let a scheduler acquire a node that has already been claimed by another scheduler
						System.out.println("[RM] " + nodeId + " is busy."); 
						continue;
					}
				}

				// TODO add more conditions/constraints such as lease time limit etc.

				// Success. Create a new lease
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
		synchronized (nodeHandler.nodesLock) {
			for (String nodeId: leaseRequest.getNodes()) {
				if (nodeHandler.nodes.get(nodeId) == null || busyNodes.get(nodeId) == null) {
					continue;
				}
				if (busyNodes.get(nodeId).getScheduler() != null && 
						busyNodes.get(nodeId).getScheduler().equals(leaseRequest.getSchedulerName())) {
					// Free the node only if it is currently owned by the scheduler
					busyNodes.remove(nodeId);
				}
			}
		}
		return true;
	}
}
