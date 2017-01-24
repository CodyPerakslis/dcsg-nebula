package edu.umn.cs.Nebula.node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URL;
import java.util.LinkedList;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import edu.umn.cs.Nebula.request.NodeRequest;
import edu.umn.cs.Nebula.request.NodeRequestType;

public abstract class Node {
	protected static final Gson gson = new Gson();

	protected static NodeInfo nodeInfo;
	protected static NodeType type;
	
	// A list of neighboring nodes and its lock
	protected static LinkedList<String> neighbors = new LinkedList<String>();
	protected static final Object neighborsLock = new Object();
		
	/**
	 * Get a node information (id, ip, latitude, longitude)
	 */
	protected static void getNodeInformation() {
		// Get the location of the ip address
		String line = null;
		String data = "";
		
		try {
			// get the node's ip address
			URL ipLookupURL;
			ipLookupURL = new URL(String.format("http://ipinfo.io/json"));
			HttpURLConnection ipLookupConn = (HttpURLConnection) ipLookupURL.openConnection();
			ipLookupConn.setRequestMethod("GET");
			ipLookupConn.connect();

			BufferedReader br = new BufferedReader(new InputStreamReader(ipLookupConn.getInputStream()));
			while((line = br.readLine()) != null) {
				data += line;
			}
		} catch(IOException e) {
			System.out.println("[NODE] Failed connecting to ip look up service: " + e);
		}
		JsonParser parser = new JsonParser();
		JsonObject jsonData = parser.parse(data).getAsJsonObject();
		if (jsonData.get("loc") == null) {
			// location not found
			System.out.println("Location not found: " + jsonData);
		} else {
			String[] returnedCoordinate = jsonData.get("loc").toString().replace("\"", "").split(",");
			String ip = jsonData.get("ip").toString();
			ip = ip.substring(1, ip.length()-1);
			nodeInfo = new NodeInfo(ip, ip, 
					Float.parseFloat(returnedCoordinate[0]), 
					Float.parseFloat(returnedCoordinate[1]),
					type);
		}	
	}
	
	/**
	 * Periodically send a heartbeat to the master. 
	 * The heartbeat message is on the form of @NodeInfo.
	 * The response contains a list of neighboring nodes.
	 * 
	 * @author albert
	 */
	protected static class Ping implements Runnable {
		int interval = 3000; // in milliseconds
		String master;
		int port;

		public Ping(String master, int port) {
			this.master = master;
			this.port = port;
		}
		
		@Override
		public void run() {
			NodeRequest request = new NodeRequest(nodeInfo, NodeRequestType.ONLINE);
			BufferedReader in = null;
			PrintWriter out = null;
			Socket socket = null;

			while (true) {
				try {
					// send a heartbeat to the Job Manager
					socket = new Socket(master, port);
					out = new PrintWriter(socket.getOutputStream());
					in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					out.println(gson.toJson(request));
					out.flush();
					synchronized (neighborsLock) {
						// update the list of neighboring nodes
						neighbors = gson.fromJson(in.readLine(), new TypeToken<LinkedList<String>>() {}.getType());
						neighbors.remove(nodeInfo.getId()); // remove myself from the list
					}
				} catch (IOException e) {
					System.out.println("[" + nodeInfo.getId() + "] Ping failed: " + e);
				} finally {
					try {
						if (in != null)
							in.close();
						if (out != null)
							out.close();
						if (socket != null)
							socket.close();
					} catch (IOException e) {
						System.err.println("[" + nodeInfo.getId() + "] Failed closing streams/socket: " + e);
					}
				}
				try {
					Thread.sleep(interval);
				} catch (InterruptedException e) {
					return;
				}
			}
		}
	}
}
