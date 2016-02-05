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
import edu.umn.cs.Nebula.model.Schedule;
import edu.umn.cs.Nebula.model.Task;
import edu.umn.cs.Nebula.request.ApplicationRequest;
import edu.umn.cs.Nebula.request.ScheduleRequest;
import edu.umn.cs.Nebula.request.TaskRequest;

public class ApplicationManager {
	// database configuration
	private static String username = "dcsg";
	private static String password = "dcsgumn";
	private static String serverName = "localhost";
	private static String dbName = "nebula";
	private static int dbPort = 3307;
	private static DatabaseConnector dbConn;

	private static Schedule schedule;
	private static final Object scheduleLock = new Object();

	private static final int port = 6421;
	private static final int poolSize = 20;

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

		schedule = new Schedule();
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
	 * Get a list of {incomplete} applications.
	 * 
	 * @param type
	 * @param incomplete
	 * @return
	 */
	private static ArrayList<Integer> getApplicationIds(ApplicationType type, boolean complete) {
		ArrayList<Integer> result = new ArrayList<Integer>();

		ResultSet rs = dbConn.selectQuery("SELECT id FROM application WHERE type=" + type + ", AND complete=" + complete + ";");

		try {
			while (rs.next()) {
				result.add(rs.getInt(1));
			}
		} catch (SQLException e) {
			System.out.println("[AM] Failed getting application id: " + e);
		}

		return result;
	}

	private static Application getApplication(int appId) {
		Application app = null;

		String appName;
		ApplicationType type;
		int priority;
		boolean active;
		boolean complete;
		// Parse application info
		ResultSet rs = dbConn.selectQuery("SELECT * FROM application WHERE id=" + appId + ";");
		try {
			while (rs.next()) {
				// this should only go once since there should only be 1 element
				appName = rs.getString("name");
				type = ApplicationType.valueOf(rs.getString("type"));
				priority = rs.getInt("priority");
				active = rs.getBoolean("active");
				complete = rs.getBoolean("complete");

				app = new Application(appId, appName, type, priority);
				app.setComplete(complete);
				app.setActive(active);
				break;
			}
		} catch (SQLException e) {
			System.out.println("[AM] Failed getting application: " + e);
			return null;
		}

		int jobId;
		int depId;
		JobType jobType;
		String exeFilename;
		String filename;
		// Parse jobs info for the application
		rs = dbConn.selectQuery("SELECT J.id, J.type, J.priority, J.active, J.complete, J.exefilename, JF.filename, D.dep_id"
				+ " FROM job J, job_file JF, dependency D WHERE J.app_id=" + appId + " AND J.id = JF.id AND J.id = D.id;");
		try {
			while (rs.next()) {
				jobId = rs.getInt("id");
				jobType = JobType.valueOf(rs.getString("type"));
				priority = rs.getInt("priority");
				active = rs.getBoolean("active");
				complete = rs.getBoolean("complete");
				exeFilename = rs.getString("exe_filename");
				filename = rs.getString("filename");
				depId = rs.getInt("dep_id");

				if (app.getJob(jobId) != null) {
					if (!app.getJob(jobId).getFileList().contains(filename)) {
						app.getJob(jobId).addFile(filename);;
					}
					if (!app.getJob(jobId).getDependencies().contains(depId)) {
						app.getJob(jobId).addDependency(depId);
					}
				} else {
					Job job = new Job(jobId, appId, priority, jobType);
					job.setComplete(complete);
					job.setActive(active);
					job.setExeFile(exeFilename);
					job.addFile(filename);
					job.addDependency(depId);
					app.addJob(job);
				}
			}
		} catch (SQLException e) {
			System.out.println("[AM] Failed getting jobs for application " + appId + ": " + e);
			return null;
		}

		int taskId;
		String completingNode;
		String status;
		// Parse tasks info for each job
		for (int id: app.getJobs().keySet()) {
			rs = dbConn.selectQuery("SELECT id, completing_node, active, complete, status"
					+ " FROM task WHERE job_id =" + id + ";");
			try {
				while (rs.next()) {
					taskId = rs.getInt("id");
					active = rs.getBoolean("active");
					complete = rs.getBoolean("complete");
					completingNode = rs.getString("completingNode");
					status = rs.getString("status");

					Task task = new Task(taskId, id);
					task.setActive(active);
					task.setComplete(complete);
					task.setCompletedBy(completingNode);
					task.setStatus(status);
					app.getJob(id).addTask(task);
				}
			} catch (SQLException e) {
				System.out.println("[AM] Failed getting tasks for job " + id + ": " + e);
				return null;
			}
		}

		return app;
	}

	/**
	 * Cancel running application, change the 'active' value of the application into 0 (false).
	 * 
	 * @param request
	 * @return
	 */
	private static boolean cancelApplication(ApplicationRequest request) {
		if (request.getApplicationId() < 0)
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
		if (request.getApplicationId() < 0)
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
		return new Task(taskId, job.getId(), filename, job.getExeFile());
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

	private static void updateJob(int jobId) {
		// check if all tasks for the job have been completed
		String query = "SELECT COUNT(*) FROM task WHERE complete=0 AND job_id = " + jobId + ";";
		ResultSet rs = dbConn.selectQuery(query);
		try {
			int numUnfinishedTasks = rs.getInt(1);
			if (numUnfinishedTasks < 1) {
				// set the job to be completed and activate any dependency jobs
				query = "UPDATE job SET active=0, complete=1, last_modified=" + new Date().toString()
						+ " WHERE id=" + jobId + ";";
				dbConn.updateQuery(query);
				query = "UPDATE job SET active=1, last_modified=" + new Date().toString()
						+ " WHERE complete=0 AND active=0 AND id IN "
						+ " (SELECT dep_id FROM dependency WHERE id=" + jobId + ";";
				dbConn.updateQuery(query);

				// get the application id and update it if all of its jobs are completed
				query = "SELECT app_id FROM job WHERE id = " + jobId + ";";
				rs = dbConn.selectQuery(query);
				int appId = rs.getInt(1);			

				updateApp(appId);
			}
		} catch (SQLException e) {
			System.out.println("[AM] Failed updating job status: " + e);
		}
	}

	private static void updateApp(int appId) {
		// check if all jobs for the app have been completed
		String query = "SELECT COUNT(*) FROM job WHERE complete=0 AND app_id = " + appId + ";";
		ResultSet rs = dbConn.selectQuery(query);
		try {
			int numUnfinishedJobs = rs.getInt(1);
			if (numUnfinishedJobs < 1) {
				// set the job to be completed and activate any dependency jobs
				query = "UPDATE app SET active=0, complete=1, last_modified=" + new Date().toString()
						+ " WHERE id=" + appId + ";";
				dbConn.updateQuery(query);
			}
		} catch (SQLException e) {
			System.out.println("[AM] Failed updating app status: " + e);
		}
	}

	private static class RequestThread implements Runnable {
		private final Socket clientSock;

		public RequestThread(Socket sock) {
			clientSock = sock;
		}

		@Override
		public void run() {
			ArrayList<Integer> applicationIds = null;
			BufferedReader in = null;
			PrintWriter out = null;
			Gson gson = new Gson();

			try {
				in = new BufferedReader(new InputStreamReader(clientSock.getInputStream()));
				out = new PrintWriter(clientSock.getOutputStream());

				ApplicationRequest appRequest = gson.fromJson(in, ApplicationRequest.class);
				TaskRequest taskRequest = gson.fromJson(in, TaskRequest.class);
				ScheduleRequest scheduleRequest = gson.fromJson(in, ScheduleRequest.class);

				boolean success = false;

				// Application request handler
				if (appRequest != null && appRequest.getType() != null) {
					switch (appRequest.getType()) {
					case CREATE:
						success = generateApplication(appRequest);
						out.println(gson.toJson(success));
						break;
					case CANCEL:
						success = cancelApplication(appRequest);
						out.println(gson.toJson(success));
						break;
					case DELETE:
						success = deleteApplication(appRequest);
						out.println(gson.toJson(success));
						break;
					case GETINACTIVEAPP:
						applicationIds = getApplicationIds(appRequest.getApplicationType(), false);
						out.println(gson.toJson(applicationIds));
						break;	
					case GETALLINACTIVEAPP:
						applicationIds = getApplicationIds(appRequest.getApplicationType(), false);
						out.println(gson.toJson(applicationIds));
						break;
					case GET:
						Application app = null;
						if (appRequest.getApplicationId() >= 0) {
							app = getApplication(appRequest.getApplicationId());
						}
						out.println(gson.toJson(app));
						break;
					default:
						System.out.println("[AM] Invalid request: " + appRequest.getType());
						out.println(gson.toJson(success));
					}
				} 
				// Task request handler
				else if (taskRequest != null && taskRequest.getType() != null && taskRequest.getNodeId() != null) {
					Task task = null;
					boolean removed = false;

					switch (taskRequest.getType()) {
					case GET:	// get a task that is scheduled for the node, if any.
						synchronized(scheduleLock) {
							task = schedule.getFirstTask(taskRequest.getNodeId());
						}
						out.println(gson.toJson(task));
						break;
					case CANCEL:
						synchronized(scheduleLock) {
							removed = schedule.removeTask(taskRequest.getNodeId(), taskRequest.getTaskId());
						}
						out.println(gson.toJson(removed));
						break;
					case FINISH:	// update the status of a task
						if (taskRequest.getTaskId() >= 0) {					
							// update the task info in the DB
							String sqlStatement = "UPDATE task "
									+ "SET active=0, complete=1, completing_node=" + taskRequest.getNodeId()
									+ ", status= " +taskRequest.getStatus() + ", last_modified=" + new Date().toString()
									+ "WHERE id = " + taskRequest.getTaskId() + ";";
							removed = dbConn.updateQuery(sqlStatement);

							synchronized(scheduleLock) {
								// remove the task from the schedule
								schedule.removeTask(taskRequest.getNodeId(), taskRequest.getTaskId());
							}

							sqlStatement = "SELECT job_id FROM task WHERE id = " + taskRequest.getTaskId() + ";";
							ResultSet rs = dbConn.selectQuery(sqlStatement);
							try {
								int jobId = rs.getInt(1);
								updateJob(jobId);
							} catch (SQLException e) {
								System.out.println("[AM] Failed getting job id: " + e);
							}
						}
						out.println(gson.toJson(removed));
						break;
					default:
						System.out.println("[AM] Invalid request: " + taskRequest.getType());
						out.println(gson.toJson(success));
					}
				} 
				// Schedule request handler
				else if (scheduleRequest != null && scheduleRequest.getNodeTask() != null && !scheduleRequest.getNodeTask().isEmpty()) {
					synchronized(scheduleLock) {
						for (String nodeId: scheduleRequest.getNodeTask().keySet()) {
							schedule.addTask(nodeId, scheduleRequest.getNodeTask().get(nodeId));
						}
					}
					out.println(gson.toJson(true));
				} else {
					System.out.println("[AM] Invalid request. ");
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