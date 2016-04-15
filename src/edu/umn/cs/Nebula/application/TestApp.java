package edu.umn.cs.Nebula.application;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;

import com.google.gson.Gson;

import edu.umn.cs.Nebula.model.JobType;
import edu.umn.cs.Nebula.request.ComputeRequest;
import edu.umn.cs.Nebula.request.ComputeRequestType;

public class TestApp {

	private static String nodeIp = "134.84.121.87";
	private static int nodePort = 2020;
	private static final int bufferSize = 4 * 1024;
	private static Gson gson = new Gson();
	private static Socket socket = null;
	
	private static OutputStream out = null;
	private static InputStream in = null;
	
	private static final Object lock = new Object();
	
	private static void getFile(String filename) {
		byte[] buffer = new byte[bufferSize];
		ComputeRequest request = new ComputeRequest("appIp", JobType.MOBILE);
		request.addContent(filename);
		
		try {
			// initialize a file
			System.out.println("Creating a file.");
			File file = new File("./" + filename);
			if(!file.exists()) {
				file.createNewFile();
			} 
			FileOutputStream fos = new FileOutputStream(file, false); 
			
			// send the request to a node
			System.out.println("Sending the request to " + nodeIp);
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			request.setTimestamp(System.currentTimeMillis());
			out.println(gson.toJson(request));
			out.flush();
			
			// get the content from the node
			InputStream in = (InputStream) socket.getInputStream();
			int readByte;
			int offset = 0;
			System.out.println("Getting file from " + nodeIp);
			while ((readByte = in.read(buffer)) != -1) {
			    fos.write(buffer, offset, readByte);
			    offset += readByte;
			}
			fos.close();
			out.close();
			in.close();
			System.out.println("Success.");
		} catch (IOException e) {
			System.out.println("[APP] Failed getting a file: " + filename + " from " + socket.getLocalAddress());
			e.printStackTrace();
		}	
	}
	
	public static void main(String[] args) throws InterruptedException {
		if (nodeIp == null || nodeIp.isEmpty()) {
			System.out.println("[APP] Invalid node's ip address.");
			return;
		}
		
		Thread pingThread = new Thread(new PingThread());
		pingThread.start();
		
		// TODO get input from user
		pingThread.join();
	}
	
	private static class PingThread implements Runnable {
		private static final int pingInterval = 5000;
		PrintWriter pw;
		BufferedReader br;
		ComputeRequest request;
		ComputeRequest reply;

		private static boolean connectToNode(String ip, int port) {
			try {
				if (socket == null || !socket.isConnected()) {
					System.out.println("[APP] Connecting to " + ip + ":" + port);
					socket = new Socket(ip, port);
				} else if (socket.isConnected() && !socket.getRemoteSocketAddress().toString().equalsIgnoreCase(ip)) {
					System.out.println("[APP] Reconnecting to " + ip + ":" + port);
					socket.close();
					socket = new Socket(ip, port);
				}
				out = socket.getOutputStream();
				in = socket.getInputStream();
				System.out.println("[APP] Connected to " + nodeIp + ":" + nodePort);
				return true;
			} catch (IOException e) {
				System.out.println("[APP] Failed connecting to " + ip + ":" + port);
				e.printStackTrace();
				return false;
			}
		}
		
		@Override
		public void run() {
			String newIp = nodeIp;
			String jsonReply;
			
			while (true) {
				synchronized (lock) {
					if (socket == null || !newIp.equals(nodeIp)) {
						nodeIp = newIp;
						if (!connectToNode(nodeIp, nodePort)) {
							return;
						}
						pw = new PrintWriter(out);
						br = new BufferedReader(new InputStreamReader(in));
					}
					System.out.println("[APP] Supported by node: " + nodeIp);
					
					request = new ComputeRequest("appIp", JobType.MOBILE, ComputeRequestType.PING);
					request.setTimestamp(System.currentTimeMillis());
					pw.println(gson.toJson(request));
					pw.flush();
					try {
						jsonReply = br.readLine();
						System.out.println(jsonReply);
						reply = gson.fromJson(jsonReply, ComputeRequest.class);
						System.out.println("[APP] Latency: " + (System.currentTimeMillis() - reply.getTimestamp()) + " ms.");
						if (reply.getNodeIp() != null && !reply.getNodeIp().isEmpty()) {
							newIp = request.getNodeIp();
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
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
