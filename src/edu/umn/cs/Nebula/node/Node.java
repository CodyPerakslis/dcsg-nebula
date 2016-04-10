package edu.umn.cs.Nebula.node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import com.google.gson.Gson;

public abstract class Node {
	
	public static String id = null;
	public static String ip = null;
	
	public static void connect(String monitorUrl, NodeType type) {
		Thread pingThread = new Thread(new PingThread(monitorUrl, type));
		pingThread.start();
	}

	private static class PingThread implements Runnable {
		private final String monitorUrl;
		private final NodeType type;
		private final int pingInterval = 3000; // in milliseconds
		
		private PingThread(String monitorUrl, NodeType type) {
			this.monitorUrl = monitorUrl;
			this.type = type;
		}
		
		@Override
		public void run() {
			Gson gson = new Gson();
			BufferedReader in = null;
			
			while (true) {
				try {
					URL url = new URL(monitorUrl);
					HttpURLConnection conn = (HttpURLConnection) url.openConnection();
					conn.setRequestMethod("POST");
					conn.setDoInput(true);
					conn.setDoOutput(true);

					PrintWriter out = new PrintWriter(conn.getOutputStream());
					if (id == null) {
						out.write("requestType=ONLINE&nodeType=" + type);
					} else {
						out.write("id=" + id + "&requestType=ONLINE&nodeType=" + type);
					}
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
	}
}
