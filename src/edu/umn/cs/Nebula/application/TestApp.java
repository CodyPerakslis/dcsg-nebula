package edu.umn.cs.Nebula.application;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.umn.cs.Nebula.job.ApplicationBase;
import edu.umn.cs.Nebula.job.TaskStatus;
import edu.umn.cs.Nebula.util.Grid;

public class TestApp extends ApplicationBase {

	private static Object mapLock = new Object();
	private static Grid index = new Grid(12, 25, 49, -125, -65);

	public static void main(String[] args) throws SecurityException, IOException {
		int port = 6419;
		int poolSize = 10;

		setupLog("/home/umn_nebula/albert/TestApp.log");

		start();
		waitForRequest(port, poolSize);
	}

	/**
	 * Start listening for requests at @port.
	 */
	private static void waitForRequest(int port, int poolSize) {
		// Listening for client requests
		ExecutorService requestPool = Executors.newFixedThreadPool(poolSize);
		ServerSocket serverSocket = null;

		try {
			serverSocket = new ServerSocket(port);
			while (true) {
				requestPool.submit(new RequestHandler(serverSocket.accept()));
			}
		} catch (IOException e) {
			finish(TaskStatus.ERROR);
			return;
		} finally {
			requestPool.shutdown();
			try {
				serverSocket.close();
			} catch (IOException e) {
			}
		}
	}

	private static class RequestHandler implements Runnable {
		private final Socket clientSocket;

		public RequestHandler(Socket clientSocket) {
			this.clientSocket = clientSocket;
		}

		@Override
		public void run() {
			BufferedReader in;
			PrintWriter out;

			try {
				// TODO error checking and handler, validate
				// simulation of a dummy application
				in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
				out = new PrintWriter(clientSocket.getOutputStream());
				String[] message = in.readLine().split("::");
				String latitude = message[0].split(",")[0];
				String longitude = message[0].split(",")[1];

				synchronized (mapLock) {
					for (int i = 1; i < message.length; i++) {
						index.insertItem(message[i], Double.parseDouble(latitude), Double.parseDouble(longitude));
					}
					out.println(index.getItems(
							index.getGridLocation(Double.parseDouble(latitude), Double.parseDouble(longitude))).getFirst());
				}
				out.flush();

				out.close();
				in.close();
				clientSocket.close();
			} catch (IOException e) {
				writeToLog(e.getMessage());
			}
		}

	}
}
