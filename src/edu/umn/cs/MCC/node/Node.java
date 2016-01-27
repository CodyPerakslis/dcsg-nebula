package edu.umn.cs.MCC.node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import com.google.gson.Gson;

public abstract class Node {
	
	private static NodeInfo info = null;
	
	public static void connect(String monitorUrl, NodeType type) {
		Thread pingThread = new Thread(new PingThread(monitorUrl, type));
		pingThread.start();
	}
	
	public static NodeInfo getInfo() {
		return info;
	}

	public static void setInfo(NodeInfo info) {
		Node.info = info;
	}

	private static class PingThread implements Runnable {
		private final String monitorUrl;
		private final NodeType type;
		private final int pingInterval = 5000;
		
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
					out.write("requestType=ONLINE&nodeType=" + type);
					out.flush();
					conn.connect();
					
					if (conn.getResponseCode() == 200) {
						in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
						setInfo(gson.fromJson(in.readLine(), NodeInfo.class));
						in.close();
					} else {
						System.out.println("[NODE] Response code: " + conn.getResponseCode());
						System.out.println("[NODE] Message: " + conn.getResponseMessage());
					}
				} catch (IOException e) {
					System.out.println("[NODE] Failed connecting to Nebula: " + e);
					e.printStackTrace();
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
