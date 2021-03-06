package edu.umn.cs.Nebula.node;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.umn.cs.Nebula.request.DSSRequest;
import edu.umn.cs.Nebula.request.DSSRequestType;
import edu.umn.cs.Nebula.util.LRUCache;

public class StorageNode extends Node {
	/* Connection configuration */
	private static final String dssMasterServer = "134.84.121.87";
	private static final int dssMasterPort = 6413;
	private static final int requestPort = 2021;
	private static final int fileTransferPort = 2022;
	private static final int maxTrial = 5;
	
	/* File/storage configuration */
	private static String fileDirectory = "/home/umn_nebula/albert/";
	private static final int bufferSize = 1024 * 64;
	private static final int timeout = 5000;
	private static LRUCache<String> cache;
	
	private static final boolean DEBUG = true;
	
	/**
	 * =========================================================================
	 */
	
	public static void main(String args[]) throws IOException {	
		if (args.length > 0) {
			fileDirectory = args[0];
		}
		nodeInfo.setNodeType(NodeType.STORAGE);
		getNodeInformation();
		
		// Connect to the DSS Master
		Thread ping = new Thread(new Ping(dssMasterServer, dssMasterPort));
		ping.start();
		
		cache = new LRUCache<String>(20);
		
		// Listen for tasks
		int poolSize = 10;
		listenForRequests(poolSize, requestPort);
	}

	/**
	 * Listen for tasks from the Job Manager.
	 * 
	 * @throws IOException
	 */
	protected static void listenForRequests(int poolSize, int port) throws IOException {
		ExecutorService requestPool = Executors.newFixedThreadPool(poolSize);
		ServerSocket serverSocket = null;

		try {
			serverSocket = new ServerSocket(port);
			if (DEBUG)
				System.out.println("[" + nodeInfo.getId() + "] Listening for client requests at port " + port);
			while (true) { // listen for client requests
				requestPool.submit(new DSSHandlerThread(serverSocket.accept()));
			}
		} catch (IOException e) {
			System.err.println("[" + nodeInfo.getId() + "] Failed listening: " + e);
		} finally {
			requestPool.shutdown();
			if (serverSocket != null)
				serverSocket.close();
		}
	}
	
	/**
	 * DSS REQUEST HANDLERS
	 * =========================================================================
	 */
	
	/**
	 * Report to the Nebula DSS Master that the node stores a new file.
	 * 
	 * @param namespace
	 * @param filename
	 * @return
	 */
	private static boolean reportNewFile(String namespace, String filename) {
		boolean success = false;
		Socket socket = null;
		BufferedReader in = null;
		PrintWriter out = null;

		try {
			// Connecting to DSS Master
			socket = new Socket(dssMasterServer, dssMasterPort);
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			out = new PrintWriter(socket.getOutputStream());

			// Send a NEW message to the DSS Master
			System.out.println("[DSS] Reporting new file to the master.");
			DSSRequest request = new DSSRequest(DSSRequestType.NEW, namespace, filename);
			request.setNodeId(nodeInfo.getId());
			out.println(gson.toJson(request));
			out.flush();

			// Get the reply from the DSS Master
			success = gson.fromJson(in.readLine(), Boolean.class);
			System.out.println("[DSS] Done reporting to the master.");
		} catch (IOException e) {
			System.err.println("[DSS] Error: " + e);
		} finally {
			try {
				if (in != null) in.close();
				if (out != null) out.close();
				if (socket != null) socket.close();
			} catch (IOException e) {
				System.err.println("[DSS] Failed to close streams or socket: " + e);
			}
		}
		return success;
	}

	/**
	 * This method set up a file for downloading.
	 * 
	 * @param namespace The file namespace
	 * @param filename 	The file name
	 * @return whether the file has been successfully set up
	 */
	private static String createNewFile(String namespace, String filename) {
		if (namespace == null || namespace.isEmpty() || filename == null || filename.isEmpty()) {
			return null;
		}

		File directory = new File(fileDirectory);
		// Check if the directory exists, create it if it does not exist
		if (!directory.exists()) {
			directory.mkdir();
			System.out.println("[DSS] New directory: " + directory.getAbsolutePath());
		}

		// Do not replace the file if it exists, create a newer version of the file
		File uploadedFile = new File(fileDirectory + "/" + namespace + "-" + filename);
		for (int idx = 1; uploadedFile.exists(); idx++) {
			String newName = idx + "_" + filename;
			uploadedFile = new File(fileDirectory + "/" + namespace + "-" + newName);
		}
		try {
			uploadedFile.createNewFile();
		} catch (IOException e) {
			System.out.println("[DSS] Failed creating a new file: " + uploadedFile.getAbsolutePath());
			return null;
		}

		return namespace + "-" + filename;
	}

	/**
	 * Delete a file if exists.
	 * This request should only be received from the storage Master.
	 * 
	 * @param namespace
	 * @param filename
	 * @return
	 */
	private static boolean handleDelete(String namespace, String filename) {
		File deletedFile = new File(fileDirectory + "/" + namespace + "-" + filename);

		if (namespace == null || filename == null || !(deletedFile.exists())) {
			return false;
		}

		try {
			Files.deleteIfExists(deletedFile.toPath());
		} catch (IOException e) {
			System.out.println("[DSS] Failed deleting file: " + e);
			return false;
		}

		return true;
	}

	/**
	 * Upload a file specified by the arguments and send it to another location.
	 * 
	 * @param namespace
	 * @param filename
	 * @return
	 */
	private static boolean uploadFile(String namespace, String filename, String destination) {
		boolean success = false;
		FileInputStream in = null;
		OutputStream out = null;
		Socket socket = null;

		File file = new File(fileDirectory + "/" + namespace + "-" + filename);
		System.out.println("[DSS] Uploading " + file + " to " + destination);

		try {
			socket = new Socket(destination, fileTransferPort);
			in = new FileInputStream(file);
			out = socket.getOutputStream();

			// Read the file and send it to the stream
			byte[] buffer = new byte[bufferSize];
			int bytesRead;
			while ((bytesRead = in.read(buffer)) > 0) {
				out.write(buffer, 0, bytesRead);
			}
			success = true;
			System.out.println("[DSS] File upload complete.");
		} catch (IOException e) {
			System.out.println("[DSS] Exception in uploading file: " + e);
			success = false;
		} finally {
			try {
				if (in != null) in.close();
				if (out != null) out.close();
				if (socket != null) socket.close();
			} catch (IOException e) {
				System.out.println("[DSS] Failed closing stream/socket" + e);
			}
		}
		return success;
	}

	/**
	 * Download a file from stream at {@code fileTransferPort} and write it to the
	 * designated file specified in the arguments.
	 * 
	 * @param namespace
	 * @param filename
	 * @return
	 */
	private static boolean downloadFile(String namespace, String filename) {
		InputStream in = null;
		FileOutputStream out = null;
		ServerSocket serverSocket = null;
		Socket clientSocket = null;
		boolean success = false;

		File file = new File(fileDirectory + "/" + namespace + "-" + filename);
		// System.out.println("[DSS] Ready to download " + file);

		try {
			// Open a connection
			serverSocket = new ServerSocket(fileTransferPort);
			serverSocket.setSoTimeout(timeout);
			clientSocket = serverSocket.accept();

			out = new FileOutputStream(file);
			in = clientSocket.getInputStream();

			// Read data from the stream and write it to the file
			byte[] buffer = new byte[bufferSize];
			int bytesRead;
			while ((bytesRead = in.read(buffer)) > 0) {
				out.write(buffer, 0, bytesRead);
				out.flush();
			}
			System.out.println("[DSS] File download complete.");

			// Report to Nebula DSS Master
			int numTries = 0;
			while (!(success = reportNewFile(namespace, filename)) && numTries < maxTrial) {
				numTries++;
				Thread.sleep(1000);
			}

			// If failed to report, remove the file. 
			if (!success && numTries >= maxTrial) {
				System.out.println("[DSS] Failed reporting to the DSS Master. Removing the file.");
				handleDelete(namespace, filename);
			}
		} catch (IOException | InterruptedException e) {
			System.out.println("[DSS] Exception in downloading a file: " + e);
			success = false;
		} finally {
			try {
				if (in != null) in.close();
				if (out != null) out.close();
				if (clientSocket != null) clientSocket.close();
				if (serverSocket != null) serverSocket.close();
			} catch (IOException e) {
				System.out.println("[DSS] Failed closing stream/socket" + e);
			}
		}
		return success;
	}

	/**
	 * Download file from a given URL. Report to the DSS Master when the download finishes.
	 * 
	 * @param namespace	The file namespace
	 * @param filename 	The file name
	 * @param url		The file's url
	 * @return success
	 */
	private static boolean downloadFromURL(String namespace, String filename, String url) {
		URL fileUrl;
		FileOutputStream out = null;
		InputStream in = null;
		boolean success = false;

		File file = new File(fileDirectory + "/" + namespace + "-" + filename);
		// System.out.println("[DSS] Ready to download " + file);

		try {
			// Establish a connection to the url
			fileUrl = new URL(url);
			HttpURLConnection conn = (HttpURLConnection) fileUrl.openConnection();
			int responseCode = conn.getResponseCode();

			if (responseCode == HttpURLConnection.HTTP_OK) {
				String contentType = conn.getContentType();
				int contentLength = conn.getContentLength();

				System.out.println("[DSS] File Content-Type = " + contentType);
				System.out.println("[DSS] File Content-Length = " + contentLength);

				// Opens input stream from the HTTP connection
				in = conn.getInputStream();
				out = new FileOutputStream(file);

				// Reading the file
				int bytesRead = -1;
				byte[] buffer = new byte[bufferSize];
				while ((bytesRead = in.read(buffer)) != -1) {
					out.write(buffer, 0, bytesRead);
				}
				System.out.println("[DSS] File download from " + url + " complete.");

				// Report to Nebula DSS Master
				int numTries = 0;
				while (!(success = reportNewFile(namespace, filename)) && numTries < maxTrial) {
					numTries++;
					Thread.sleep(1000);
				}

				// If failed to report, remove the file. 
				if (!success && numTries >= maxTrial) {
					System.out.println("[DSS] Failed reporting to the DSS Master. Removing the file.");
					handleDelete(namespace, filename);
				}
			} else {
				System.out.println("[DSS] Download URL fails. HTTP response code: " + responseCode);
			}
			conn.disconnect();
		} catch (IOException | InterruptedException e) {
			System.out.println("[DSS] Failed downloading file: " + e);
		} finally {
			try {
				if (in != null) in.close();
				if (out != null) out.close();
			} catch (IOException e) {
				System.out.println("[DSS] Failed closing stream/socket" + e);
			}
		}

		return success;
	}

	/**
	 * Download a file from another DSS node
	 * 
	 * @param namespace	The namespace
	 * @param filename 	The filename
	 * @param ip		The ip address of the DSS node storing the file
	 * @return	whether the file has been successfully downloaded
	 */
	private static boolean downloadFromDSS(String namespace, String filename, String ip) {
		BufferedReader in = null;
		PrintWriter out = null;
		boolean success = false;

		Socket socket = null;
		DSSRequest request = null;

		if (namespace == null || namespace.isEmpty() || filename == null || filename.isEmpty() || ip == null || ip.isEmpty()) {
			System.out.println("[DSS] Invalid information for downloading file from DSS.");
			return false;
		}

		try {
			// Connecting to the another DSS node
			socket = new Socket(ip, requestPort);
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			out = new PrintWriter(socket.getOutputStream());

			System.out.println("[DSS] Getting a file from the DSS: " + ip);
			request = new DSSRequest(DSSRequestType.UPLOAD, namespace, filename);
			request.setNodeId(ip);
			out.println(gson.toJson(request));
			out.flush();
			success = gson.fromJson(in.readLine(), Boolean.class);

			// Failed connecting to the DSS
			if (!success) {
				System.out.println("[DSS] Failed getting file from DSS:" + ip);
				if (in != null) in.close();
				if (out != null) out.close();
				if (socket != null) socket.close();
				return false;
			}

			// Download from the DSS
			success = downloadFile(namespace, filename);
		} catch (IOException e) {
			System.err.println("[DSS] Error: " + e);
			success = false;
		} finally {
			try {
				if (in != null) in.close();
				if (out != null) out.close();
				if (socket != null) socket.close();
			} catch (IOException e) {
				System.err.println("[DSS] Failed to close streams/socket: " + e);
			}
		}
		return success;
	}

	private static class DSSHandlerThread implements Runnable {
		private final Socket clientSock;

		public DSSHandlerThread(Socket sock) {
			clientSock = sock;
		}

		@Override
		public void run() {
			BufferedReader in = null;
			PrintWriter out = null;
			boolean response = false;

			try {
				in = new BufferedReader(new InputStreamReader(clientSock.getInputStream()));
				out = new PrintWriter(clientSock.getOutputStream());

				DSSRequest request = gson.fromJson(in.readLine(), DSSRequest.class);
				if (request != null && request.getNamespace() != null && request.getFilename() != null) {
					String filenameCombination = null;
					System.out.println("[DSS] Handling " + request.getType() + " request");

					switch (request.getType()) {
					case DOWNLOAD:
						filenameCombination = createNewFile(request.getNamespace(), request.getFilename());
						if (filenameCombination != null) {
							response = downloadFile(filenameCombination.split("-")[0], filenameCombination.split("-")[1]);
						} break;
					case DOWNLOADURL:
						filenameCombination = createNewFile(request.getNamespace(), request.getFilename());
						if (request.getNote() != null && filenameCombination != null) {
							response = downloadFromURL(
									filenameCombination.split("-")[0], 
									filenameCombination.split("-")[1],
									request.getNote());
						} break;
					case DOWNLOADDSS:
						filenameCombination = createNewFile(request.getNamespace(), request.getFilename());
						if (request.getNote() != null && filenameCombination != null) {
							response = downloadFromDSS(
									filenameCombination.split("-")[0], 
									filenameCombination.split("-")[1],
									request.getNote());
						} break;
					case UPLOAD:
						filenameCombination = createNewFile(request.getNamespace(), request.getFilename());
						if (request.getNodeId() != null && filenameCombination != null) {
							response = uploadFile(
									filenameCombination.split("-")[0], 
									filenameCombination.split("-")[1],
									request.getNodeId());
						} break;
					case CACHE:
						File file = new File(fileDirectory + "/" + request.getNamespace() + "-" + request.getFilename());
						if (file.exists()) {
							FileInputStream fis = new FileInputStream(file);
							byte[] buffer = new byte[bufferSize];
							String data = "";
							while (fis.read(buffer) > 0) {
								data += new String(buffer);
							}
							cache.add(file.getName(), data);
							fis.close();
							response = true;
							System.out.println("[DSS] Cached: " + cache.getKeys());
						} break;
					default:
						System.out.println("[DSS] Invalid request: " + request.getType());
					}
				}
				out.println(gson.toJson(response));
				out.flush();
			} catch (IOException e) {
				System.err.println("Error: " + e);
			} finally {
				try {
					if (in != null) in.close();
					if (out != null) out.close();
					if (clientSock != null) clientSock.close();
				} catch (IOException e) {
					System.err.println("Failed to close streams or socket: " + e);
				}
			}
		}
	}
}
