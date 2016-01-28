package edu.umn.cs.Nebula.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.gson.Gson;

import edu.umn.cs.Nebula.model.Application;
import edu.umn.cs.Nebula.model.ApplicationType;
import edu.umn.cs.Nebula.model.DatabaseConnector;
import edu.umn.cs.Nebula.model.Job;
import edu.umn.cs.Nebula.model.JobType;
import edu.umn.cs.Nebula.model.Task;
import edu.umn.cs.Nebula.request.ApplicationRequest;

public class ApplicationManager {
	// database configuration
	private static String username = "dcsg";
	private static String password = "dcsgumn";
	private static String serverName = "localhost";
	private static String dbName = "nebula";
	private static int dbPort = 3307;
	private static DatabaseConnector dbConn;
	
	private static final int port = 6421;
	private static final int poolSize = 5;
	

	public static void main(String args[]) {		
		if (args.length < 5) {
			System.out.println("[AM] Using the default DB configuration.");
		} else {
			username = args[0];
			password = args[1];
			serverName = args[2];
			dbName = args[3];
			dbPort = Integer.parseInt(args[4]);
		}
		
		System.out.print("[AM] Setting up Database connection ...");	
		dbConn = new DatabaseConnector(username, password, serverName, dbName, dbPort);
		if (dbConn == null) {
			System.out.println("[FAILED]");
			return;
		} else {
			System.out.println("[OK]");
		}
		
		ExecutorService requestPool = Executors.newFixedThreadPool(poolSize);
		ServerSocket serverSock = null;
		
		try {
			serverSock = new ServerSocket(port);
			System.out.println("[AM] Listening for Application requests on port " + port);
			while (true) {
				requestPool.submit(new RequestThread(serverSock.accept()));
			}
		} catch (IOException e) {
			System.err.println("[AM] Failed to establish listening socket: " + e);
		} finally {
			requestPool.shutdown();
			if (serverSock != null) {
				try {
					serverSock.close();
				} catch (IOException e) {
					System.err.println("[AM] Failed to close listening socket");
				}
			}
		}
	}

	/**
	 * Get a new id for a new application.
	 * @param tablename
	 * @return
	 */
	private static int getNewId(String tablename) {
		ResultSet rs = dbConn.selectQuery("SELECT MAX(id) FROM " + tablename + ";");
		int id;

		try {
			rs.next();
			id = rs.getInt(1) + 1;
		} catch (SQLException e) {
			return -1;
		}
		return id;
	}

	/**
	 * Cancel running application, change the 'active' value of the application into 0 (false).
	 * 
	 * @param request
	 * @return
	 */
	private static boolean cancelApplication(ApplicationRequest request) {
		if (request.getApplicationId() == null)
			return false;

		String sqlStatement = "UPDATE application SET active = " + 0 + " WHERE id = " + request.getApplicationId() + ";";
		return dbConn.updateQuery(sqlStatement);
	}

	/**
	 * Remove a specific application from the database.
	 * 
	 * @param request
	 * @return
	 */
	private static boolean deleteApplication(ApplicationRequest request) {
		if (request.getApplicationId() == null)
			return false;

		String sqlStatement = "DELETE FROM application WHERE id =" + request.getApplicationId() + ";";
		return dbConn.updateQuery(sqlStatement);
	}

	/**
	 * Add a new application into the database.
	 * 
	 * @param request
	 * @return
	 */
	private static boolean generateApplication(ApplicationRequest request) {
		boolean result = false;
		ArrayList<Job> jobs;
		int id = getNewId("application");

		if (id < 0) {
			id = 1;
		}

		String name = request.getApplicationName();
		ApplicationType applicationType = request.getApplicationType();

		Application application = new Application(id, name, applicationType);
		String sqlStatement = "INSERT INTO application (id, name, priority, active, complete, last_modified, type) " +
				"VALUES (" + id + ", '" + name + "', " + application.getPriority() + ", " + 
				1 + ", " + 0 + ", '" + new Date().toString() + "', '" + applicationType + "');";

		if (!dbConn.updateQuery(sqlStatement)) {
			System.out.println("[AM] Failed creating a new application.");
			return result;
		}

		switch (applicationType) {
		case MAPREDUCE:
			jobs = createMapReduceJobs(request, id, application.getPriority());
			if (jobs == null || jobs.size() < 2) {
				return result;
			}
			application.addJob(jobs.get(0));
			application.addJob(jobs.get(1));
			result = true;
			break;
		case MOBILE:
			// TODO Handle mobile request
			break;
		default:
			System.out.println("[AM] Undefined application type.");
		}
		return true;
	}

	/**
	 * Create a job object.
	 * 
	 * @param jobId
	 * @param appId
	 * @param priority
	 * @param jobType
	 * @param exeFile
	 * @param numNodes
	 * @return
	 */
	private static Job generateJob(int jobId, int appId, int priority, JobType jobType, String exeFile, int numNodes) {
		Job job = new Job(jobId, appId, priority, jobType);
		job.setExeFile(exeFile);
		job.setNumNodes(numNodes);
		return job;
	}

	/**
	 * Create a task object.
	 * 
	 * @param job
	 * @param filename
	 * @return
	 */
	private static Task generateTask(Job job, String filename) {
		int taskId = getNewId("task");

		if (taskId < 0) {
			taskId = 1;
		}

		dbConn.updateQuery("INSERT INTO task (id, job_id, active, complete, last_modified, filename) " +
				"VALUES (" + taskId + ", " + job.getId() + ", " + 1 + ", " + 0 + ", '" + new Date().toString() + "', '" + filename + "');");
		dbConn.updateQuery("INSERT INTO job_file (id, filename) " +
				"VALUES (" + job.getId() + ", '" + filename +"');");
		return new Task(taskId, filename);
	}

	/**
	 * Create a MapReduce job from an application.
	 * 
	 * @param request
	 * @param applicationId
	 * @param priority
	 * @return
	 */
	private static ArrayList<Job> createMapReduceJobs(ApplicationRequest request, int applicationId, int priority) {
		boolean successQuery = false;
		ArrayList<Job> jobs = new ArrayList<Job>();

		// Create the MAP job
		int mapJobId = getNewId("job");
		if (mapJobId < 0) {
			mapJobId = 1;
		}

		String mapExe = request.getJobExecutable(JobType.MAP);
		ArrayList<String> mapInputs = request.getJobInputs(JobType.MAP);

		if (mapExe == null || mapInputs == null || mapInputs.size() < 1) {
			System.out.println("[MR] Failed creating a MAP job.");
			return null;
		}
		Job mapJob = generateJob(mapJobId, applicationId, priority, JobType.MAP, mapExe, mapInputs.size());
		// Save the job to the Database
		successQuery = dbConn.updateQuery("INSERT INTO job (id, type, app_id, active, complete, exe_filename, last_modified, num_nodes) " +
				"VALUES (" + mapJobId + ", '" + JobType.MAP + "', " + applicationId + ", " + 1 + ", " + 0 + ", '" +
				mapExe + "', '" + new Date().toString() + "', " + mapInputs.size() + ");");
		if (!successQuery) {
			System.out.println("[MR] Failed saving a MAP job.");
			return null;
		}
		// Create MAP tasks
		for (String filename: mapInputs) {
			mapJob.addFile(filename);
			mapJob.addTask(generateTask(mapJob, filename));
		}

		// Create the REDUCE job
		int redJobId = getNewId("job");
		if (redJobId < 0) {
			redJobId = 1;
		}

		String redExe = request.getJobExecutable(JobType.REDUCE);
		ArrayList<String> redInputs = request.getJobInputs(JobType.REDUCE);

		if (redExe == null || redInputs == null || redInputs.size() < 1) {
			System.out.println("[MR] Failed creating a REDUCE job.");
			return null;
		}
		Job redJob = generateJob(redJobId, applicationId, priority, JobType.REDUCE, redExe, redInputs.size());
		redJob.addDependency(mapJobId);
		// Save the job to the Database
		successQuery = dbConn.updateQuery("INSERT INTO job (id, type, app_id, active, complete, exe_filename, last_modified, num_nodes) " +
				"VALUES (" + redJobId + ", '" + JobType.REDUCE + "', " + applicationId + ", " + 0 + ", " + 0 + ", '" +
				redExe + "', '" + new Date().toString() + "', " + redInputs.size() + ");");
		if (!successQuery) {
			System.out.println("[MR] Failed saving a REDUCE job.");
			return null;
		}
		// Create MAP tasks
		for (String filename: redInputs) {
			redJob.addFile(filename);
			redJob.addTask(generateTask(redJob, filename));
		}

		jobs.add(mapJob);
		jobs.add(redJob);
		return jobs;
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

			try {
				in = new BufferedReader(new InputStreamReader(clientSock.getInputStream()));
				out = new PrintWriter(clientSock.getOutputStream());

				ApplicationRequest request = gson.fromJson(in.readLine(), ApplicationRequest.class);
				boolean success = false;

				switch (request.getType()) {
				case CREATE:
					success = generateApplication(request);
					break;
				case CANCEL:
					success = cancelApplication(request);
					break;
				case DELETE:
					success = deleteApplication(request);
					break;
				default:
					System.out.println("[AM] Invalid request: " + request.getType());
					out.println(gson.toJson(success));
				}
				out.flush();
			} catch (IOException e) {
				System.err.println("[AM]: " + e);
			} finally {
				try {
					if (in != null) in.close();
					if (out != null) out.close();
					if (clientSock != null) clientSock.close();
				} catch (IOException e) {
					System.err.println("[AM] Failed to close streams or socket: " + e);
				}
			}
		}
	}
}
