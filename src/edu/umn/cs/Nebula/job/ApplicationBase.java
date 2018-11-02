package edu.umn.cs.Nebula.job;

import java.io.IOException;
import java.util.LinkedList;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;


public class ApplicationBase {
	private static TaskStatus status = TaskStatus.RUNNING;
	private static Thread taskUpdateThread;
	private static Logger logger = Logger.getLogger("LOG");  
	private static FileHandler fh;
	private static double load = 0.0;
	
	protected static LinkedList<String> redirectNodes = new LinkedList<String>();
	
	protected static void writeToLog(String content) {
		logger.info(content);
	}
	
	protected static void setupLog(String logName) throws SecurityException, IOException {
		fh = new FileHandler(logName);
		logger.addHandler(fh);
		SimpleFormatter formatter = new SimpleFormatter();  
        fh.setFormatter(formatter);
        //writeToLog("START LOGGING");
	}
	
	protected void updateStatus(TaskStatus status) {
		ApplicationBase.status = status;
	}
	
	protected static TaskStatus getStatus() {
		return status;
	}
	
	protected static void start() {
		taskUpdateThread = new Thread(new TaskUpdate());
		taskUpdateThread.start();
	}
	
	protected static void sendTaskUpdate(TaskStatus status, double load) {
		System.out.println(status + ":" + load);
		System.out.flush();
	}
	
	protected static void finish(TaskStatus exitStatus) {
		taskUpdateThread.interrupt();
		sendTaskUpdate(exitStatus, 0.0);
	}
	
	protected static void finish(TaskStatus exitStatus, String message) {
		taskUpdateThread.interrupt();
		sendTaskUpdate(exitStatus, 0.0);
		writeToLog("EXIT: " + exitStatus + ": " + message);
	}
	
	protected static void updateLoad(double load) {
		ApplicationBase.load = load;
	}
	
	protected static double getLoad() {
		return load;
	}
	
	private static class TaskUpdate implements Runnable {
		private final int updateInterval = 2000;
		
		@Override
		public void run() {
			while (true) {
				try {
					Thread.sleep(updateInterval);
					sendTaskUpdate(TaskStatus.RUNNING, load);
					writeToLog("LOAD: " + load);
				} catch (InterruptedException e) {}
			}
		}
	}
}
