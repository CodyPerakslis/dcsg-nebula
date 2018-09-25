package org.apache.edgent.samples.connectors.mqtt;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class PhotonDriver {
	
	private static final int listen = 5002;
	private static final int send = 1870;
	
	public static String findDevice() {
		
		String id = null;
		
		try {
			
			// Start listening on socket.
			DatagramSocket socket = new DatagramSocket(listen);
			
			System.out.println("Listening on port " + listen + "!");
			
			// Get data.
			byte[] buffer = new byte[16];
			DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
			socket.receive(packet);
			System.out.println("Recieved packet!");
			
			InetAddress address = packet.getAddress();
			String data = new String(packet.getData());
			String[] parts = data.split(":");
			
			if (parts.length > 1 && Integer.parseInt(parts[1]) == send) {
				
				// Set id.
				id = address.getHostAddress();
				
				// Register with the photon.
				//communicateWithPhoton(id, 1);
			}
			
			socket.close();
			
		} catch (IOException e) {
			
			// Failed.
			e.printStackTrace();
		}
		
		return id;
	}
	
	public static int getTemp(String id) {
		
		return Integer.parseInt(communicateWithPhoton(id, 2));
	}
	
	public static boolean alterLED(String id, int r, int g, int b) {
		
		return true;
	}
	
	public static boolean activateLED(String id) {
		
		return Boolean.parseBoolean(communicateWithPhoton(id, 3));
	}
	
	public static boolean deactivateLED(String id) {
		
		return Boolean.parseBoolean(communicateWithPhoton(id, 4));
	}
	
	public static float getEnergyLevel(String id) {
		
		return Float.parseFloat(communicateWithPhoton(id, 7));
	}
	
	public static boolean sendDebugMessage(String id, String message) {
		
		return false;
	}
	
	public static boolean debugOn(String id) {
		
		return false;
	}
	
	public static boolean debugOff(String id) {
		
		return false;
	}
	
	public static boolean ledOn(String id) {
		
		return false;
	}
	
	public static boolean ledOff(String id) {
		
		return false;
	}
	
	public static boolean wifiOn(String id) {
		
		return false;
	}
	
	public static boolean wifiOff(String id, int seconds) {
		
		boolean result;
		
		result = Boolean.parseBoolean(communicateWithPhoton(id, 10));
		result = Boolean.parseBoolean(communicateWithPhoton(id, (seconds / 1000)));
		
		return result;
	}
	
	private static String communicateWithPhoton(String address, int id) {

		String result = "";
		
		// Open a port to the device.
		try {
					
			Socket s = new Socket(address, send);
			PrintWriter write = new PrintWriter(s.getOutputStream());
			BufferedReader read = new BufferedReader(new InputStreamReader(s.getInputStream()));
			
			//System.out.println("Reading from device...");
			
			// Write out the integer id.
			byte[] b = {(byte) id};
			//long t1 = System.currentTimeMillis();
			s.getOutputStream().write(b);
			s.getOutputStream().flush();
			
			// Read in the result.
			result = read.readLine();
			//System.out.println("Pure read time = " + (System.currentTimeMillis() - t1));
			
			System.out.println("Read in value = " + result);
			
			// Close socket to prevent leaks.
			s.close();
			
		} catch (UnknownHostException e) {
					
			// TODO Auto-generated catch block
			System.out.println("HOST ERROR in photon driver.");
			
		} catch (IOException e) {
					
			// TODO Auto-generated catch block
			System.out.println("I/O ERROR in photon driver.");
		}
		
		return result == null || result.equals("") ? "0" : result;
	}
	
	private static byte[] createArgPacket(int opCode, String ... strings) {
		
		int size = 1 + strings.length;
		
		for (int i=0; i < strings.length; ++i) {
			
			size += strings[i].length();
		}
		
		byte[] message = new byte[size];
		
		message[0] = (byte) opCode;
		for (int i=0; i < strings.length; ++i) {
			
			byte[] data = strings[i].getBytes();
			for (int j=0; j < data.length; ++j) {
				
				
			}
		}
		
		return null;
	}
}
