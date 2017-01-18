package edu.umn.cs.Nebula.application;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.LinkedList;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import edu.umn.cs.Nebula.model.Coordinate;
import edu.umn.cs.Nebula.model.JobType;
import edu.umn.cs.Nebula.request.ComputeRequest;
import edu.umn.cs.Nebula.request.ComputeRequestType;
import edu.umn.cs.Nebula.request.MobileRequest;
import edu.umn.cs.Nebula.request.MobileRequestType;

public class Application {

	private static String server = "134.84.121.87";
	private static int port = 6429;
	private static final Gson gson = new Gson();

	private static String ip;
	protected static Coordinate location = new Coordinate(Double.MIN_VALUE, Double.MIN_VALUE);
	protected static LinkedList<String> nodes = new LinkedList<String>();

	public static LinkedList<String> getNodes() {
		return nodes;
	}

	public static void clearNodes() {
		nodes.clear();
	}
	
	/**
	 * Get information on (ip, latitude, longitude)
	 */
	protected static void getLocationInformation() {
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
			return;
		}
		JsonParser parser = new JsonParser();
		JsonObject jsonData = parser.parse(data).getAsJsonObject();
		if (jsonData.get("loc") == null) {
			// location not found
			System.out.println("Location not found: " + jsonData);
		} else {
			String[] returnedCoordinate = jsonData.get("loc").toString().replace("\"", "").split(",");
			ip = jsonData.get("ip").toString();
			location.setLatitude(Double.parseDouble(returnedCoordinate[0]));
			location.setLongitude(Double.parseDouble(returnedCoordinate[1]));
		}
		// create a new node object
		ip = ip.substring(1, ip.length()-1);
	}

	protected static void getLocalNodes() throws UnknownHostException, IOException {
		MobileRequest request = new MobileRequest(MobileRequestType.ALL);
		System.out.println(location.getLatitude() + ", " + location.getLongitude());

		Socket sock = new Socket(server, port);
		BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
		PrintWriter out = new PrintWriter(sock.getOutputStream());

		out.println(gson.toJson(request));
		out.flush();

		nodes = gson.fromJson(in.readLine(), new TypeToken<LinkedList<String>>() {}.getType());
		in.close();
		out.close();
		sock.close();
	}

	protected static boolean sendData(Object data, String address, int port) throws UnknownHostException, IOException {
		boolean success = false;
		
		Socket sock = new Socket(address, port);
		BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
		PrintWriter out = new PrintWriter(sock.getOutputStream());
		out.println(gson.toJson(data));
		out.flush();
		
		success = gson.fromJson(in.readLine(), Boolean.class);
		in.close();
		out.close();
		sock.close();
		
		return success;
	}
		
	
	protected static boolean sendFile(String filename, String filePath, String address, int port) throws UnknownHostException, IOException {
		boolean success = false;
		int bufferSize = 1024 * 1024;
		byte[] buffer = new byte[bufferSize];
		int bytesRead;
		FileInputStream in;
		OutputStream out;

		if (filename == null || filename.isEmpty() || filePath == null || filePath.isEmpty())
			return false;

		File file = new File(filePath);
		if (!file.exists())
			return false;
		
		ComputeRequest req = new ComputeRequest(address, JobType.MOBILE, ComputeRequestType.UPLOAD);
		req.addContent(filename);
		req.addContent(Long.toString(file.length()));
		
		Socket sock = new Socket(address, port);

		in = new FileInputStream(file);
		out = sock.getOutputStream();
		PrintWriter pw = new PrintWriter(out);
		pw.println(gson.toJson(req));
		pw.flush();
		
		// Send the file
		System.out.println("Sending file <" + filePath + ", " + file.length() + "> to " + address);
		while ((bytesRead = in.read(buffer)) != -1) {
			out.write(buffer, 0, bytesRead);
			out.flush();
		}
		success = true;

		in.close();
		out.close();
		sock.close();
		
		return success;
	}
}
