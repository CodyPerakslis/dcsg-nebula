package edu.umn.cs.Nebula.node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public abstract class Node {
	
	public static String id = null;
	public static String ip = null;
	public static Double latitude = Double.MIN_VALUE;
	public static Double longitude = Double.MIN_VALUE;
	private static final Gson gson = new Gson();
	
	public static void connect(String url, NodeType type) {
		Thread pingThread = new Thread(new PingThread(url, type));
		pingThread.start();
	}

	private static class PingThread implements Runnable {
		private final String server;
		private final NodeType type;
		private final int pingInterval = 2000; // in milliseconds
		
		private PingThread(String url, NodeType type) {
			this.server = url;
			this.type = type;
		}
		
		@Override
		public void run() {
			BufferedReader in = null;
			
			while (true) {
				try {
					URL url = new URL(server);
					HttpURLConnection conn = (HttpURLConnection) url.openConnection();
					conn.setRequestMethod("POST");
					conn.setDoInput(true);
					conn.setDoOutput(true);

					PrintWriter out = new PrintWriter(conn.getOutputStream());
					if (id == null) {
						getNodeInformation();
						System.out.println("[NODE] id: " + id + " lat: " + latitude + " lon: " + longitude);
					}
					out.write("id=" + id + "&requestType=ONLINE&nodeType=" + type + "&latitude=" + latitude + "&longitude=" + longitude);
					out.flush();
					conn.connect();
					
					if (conn.getResponseCode() == 200) {
						in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
						NodeInfo info = gson.fromJson(in.readLine(), NodeInfo.class);
						if (id == null || ip == null) {
							id = info.getId();
							ip = info.getIp();
						}
						in.close();
					} else {
						System.out.println("[NODE] Response code: " + conn.getResponseCode());
						System.out.println("[NODE] Message: " + conn.getResponseMessage());
					}
				} catch (IOException e) {
					System.out.println("[NODE] Failed connecting to Nebula: " + e);
					return;
				}
				try {
					Thread.sleep(pingInterval);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
		/**
		 * Get a node information (id, ip, latitude, longitude)
		 */
		private void getNodeInformation() {
			// Get the location of the ip address
			try {
				// get the node's ip address
				URL ipLookupURL;
				ipLookupURL = new URL(String.format("http://ipinfo.io/json"));
				HttpURLConnection ipLookupConn = (HttpURLConnection) ipLookupURL.openConnection();
				ipLookupConn.setRequestMethod("GET");
				ipLookupConn.connect();

				BufferedReader br = new BufferedReader(new InputStreamReader(ipLookupConn.getInputStream()));
				String line = null;
				String data = "";
				while((line = br.readLine()) != null) {
					data += line;
				}
				
				JsonParser parser = new JsonParser();
				JsonObject jsonData = parser.parse(data).getAsJsonObject();
				if (jsonData.get("loc") == null) {
					// location not found
					System.out.println("Location not found: " + jsonData);
				} else {
					String[] coordinate = jsonData.get("loc").toString().replace("\"", "").split(",");
					ip = jsonData.get("ip").toString();
					latitude = Double.parseDouble(coordinate[0]);
					longitude = Double.parseDouble(coordinate[1]);
				}
				// create a new node object
				ip = ip.substring(1, ip.length()-1);
				id = ip;
			} catch(IOException e) {
				System.out.println("[NODE] Failed connecting to ip look up service: " + e);
			}
		}
	}
}
