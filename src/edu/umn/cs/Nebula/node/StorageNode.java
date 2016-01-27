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

import com.google.gson.Gson;

import edu.umn.cs.Nebula.cache.LRUCache;
import edu.umn.cs.Nebula.request.DSSRequest;
import edu.umn.cs.Nebula.request.DSSRequestType;

public class StorageNode extends Node {
	private static final int poolSize = 10;
	private static final int requestPort = 2021;
	private static final int fileTransferPort = 2022;
	private static final int maxTries = 5;
	private static final String fileDirectory = "DSS";
	private static final int bufferSize = 1024 * 64;
	private static final int timeout = 10000;

	private static final String masterUrl = "localhost";
	private static final int masterPort = 6423;
	
	private static LRUCache cache;

	public static void main(String args[]) {
		if (args.length < 2) {
			System.out.println("[DSS] Invalid arguments: NebulaURL, CacheSize");
			return;
		}
		
		String nebulaUrl = args[0];
		connect(nebulaUrl, NodeType.STORAGE);

		int cacheSize = Integer.parseInt(args[1]);
		cache = new LRUCache(cacheSize);
		
		ExecutorService requestPool = Executors.newFixedThreadPool(poolSize);
		ServerSocket serverSock = null;

		try {
			serverSock = new ServerSocket(requestPort);
			System.out.println("[DSS] Listening for client requests on port " + requestPort);
			while (true) {
				requestPool.submit(new RequestThread(serverSock.accept()));
			}
		} catch (IOException e) {
			System.err.println("[DSS] Failed to establish listening socket: " + e);
		} finally {
			requestPool.shutdown();
			if (serverSock != null) {
				try {
					serverSock.close();
				} catch (IOException e) {
					System.err.println("[DSS] Failed to close listening socket");
				}
			}
		}
	}

	/**
	 * Report to the Nebula DSS Master that the node stores a new file.
	 * 
	 * @param namespace
	 * @param filename
	 * @return
	 */
	private static boolean reportNewFile(String namespace, String filename) {
		boolean success = false;
		Gson gson = new Gson();

		Socket socket = null;
		BufferedReader in = null;
		PrintWriter out = null;

		try {
			// Connecting to DSS Master
			socket = new Socket(masterUrl, masterPort);
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			out = new PrintWriter(socket.getOutputStream());

			// Send a NEW message to the DSS Master
			System.out.println("[DSS] Reporting new file to the master.");
			DSSRequest request = new DSSRequest(DSSRequestType.NEW, namespace, filename);
			request.setNodeId(id);
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
		System.out.println("[DSS] Ready to upload " + file);

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
		System.out.println("[DSS] Ready to download " + file);

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
			while (!(success = reportNewFile(namespace, filename)) && numTries < maxTries) {
				numTries++;
				Thread.sleep(1000);
			}

			// If failed to report, remove the file. 
			// This should not happen
			if (!success && numTries >= maxTries) {
				System.out.println("[DSS] Failed reporting to the DSS Master.");
				handleDelete(namespace, filename);
				System.out.println("[DSS] Removing the file.");
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
		System.out.println("[DSS] Ready to download " + file);
		
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
				System.out.println("[DSS] File downloaded");

				// Report to Nebula DSS Master
				int numTries = 0;
				while (!(success = reportNewFile(namespace, filename)) && numTries < maxTries) {
					numTries++;
					Thread.sleep(1000);
				}

				// If failed to report, remove the file. 
				// This should not happen
				if (!success && numTries >= maxTries) {
					System.out.println("[DSS] Failed reporting to the DSS Master.");
					handleDelete(namespace, filename);
					System.out.println("[DSS] Removing the file.");
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

		Gson gson = new Gson();
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
			request = new DSSRequest(DSSRequestType.GETFILE, namespace, filename);
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

	private static class RequestThread implements Runnable {
		private final Socket clientSock;

		public RequestThread(Socket sock) {
			clientSock = sock;
		}

		@Override
		public void run() {
			BufferedReader in = null;
			PrintWriter out = null;
			Gson gson = new Gson();
			boolean response = false;

			try {
				in = new BufferedReader(new InputStreamReader(clientSock.getInputStream()));
				out = new PrintWriter(clientSock.getOutputStream());

				DSSRequest request = gson.fromJson(in.readLine(), DSSRequest.class);
				if (request != null) {
					System.out.println("[DSS] Received a " + request.getType() + " request");

					if (request.getType().equals(DSSRequestType.DOWNLOAD)) {
						String filenameCombination = createNewFile(request.getNamespace(), request.getFilename());
						if (filenameCombination != null) {
							response = downloadFile(filenameCombination.split("-")[0], filenameCombination.split("-")[1]);
						}
					} else if (request.getType().equals(DSSRequestType.DOWNLOADURL) && (request.getNote() != null)) {
						String filenameCombination = createNewFile(request.getNamespace(), request.getFilename());
						if (filenameCombination != null) {
							response = downloadFromURL(
									filenameCombination.split("-")[0], 
									filenameCombination.split("-")[1],
									request.getNote());
						}
					} else if (request.getType().equals(DSSRequestType.DOWNLOADDSS) && (request.getNote() != null)) {
						String filenameCombination = createNewFile(request.getNamespace(), request.getFilename());
						if (filenameCombination != null) {
							response = downloadFromDSS(
									filenameCombination.split("-")[0], 
									filenameCombination.split("-")[1],
									request.getNote());
						}
					} else if (request.getType().equals(DSSRequestType.UPLOAD) && request.getNote() != null) {
						String filenameCombination = createNewFile(request.getNamespace(), request.getFilename());
						if (filenameCombination != null) {
							response = uploadFile(
									filenameCombination.split("-")[0], 
									filenameCombination.split("-")[1],
									request.getNote());
						}
					} else if (request.getType().equals(DSSRequestType.GETFILE)) {
						String downloader = request.getNodeId();
						String filenameCombination = createNewFile(request.getNamespace(), request.getFilename());
						if (downloader != null && filenameCombination != null || !filenameCombination.isEmpty()) {
							response = uploadFile(
									filenameCombination.split("-")[0], 
									filenameCombination.split("-")[1],
									downloader);
						}
					} else if (request.getType().equals(DSSRequestType.CACHE)) {
						// TODO cache data from the disk
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
						}
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
