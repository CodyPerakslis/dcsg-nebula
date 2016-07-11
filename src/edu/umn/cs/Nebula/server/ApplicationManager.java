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
	private static String username = "nebula";
	private static String password = "kvm";
	private static String serverName = "localhost";
	private static String dbName = "nebula";
	private static int dbPort = 3307; // set to 3306 in hemant
	private static DatabaseConnector dbConn;

	private static Schedule schedule;
	private static final Object scheduleLock = new Object();
	private static final Gson gson = new Gson();

	private static final int port = 6421;
	private static final int poolSize = 20;

	public static void main(String args[]) {		
		if (args.length < 5) {
			System.out.println("[AM] Using default DB configuration");
		} else {
			username = args[0];
			password = args[1];
			serverName = args[2];
			dbName = args[3];
			dbPort = Integer.parseInt(args[4]);
		}

		// Setup the database connection
		System.out.print("[AM] Connecting to the database ...");	
		dbConn = new DatabaseConnector(username, password, serverName, dbName, dbPort);
		if (!dbConn.connect()) {
			System.out.println("[FAILED]");
			return;
		} else {
			System.out.println("[OK]");
		}

		schedule = new Schedule();
		// Setup thread list to handle application requests
		ExecutorService requestPool = Executors.newFixedThreadPool(poolSize);
		ServerSocket serverSock = null;
		try {
			serverSock = new ServerSocket(port);
			System.out.println("[AM] Listening for Application requests on port " + port);
			while (true) {
				requestPool.submit(new RequestHandlerThread(serverSock.accept()));
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
	 * Get an ID for a new application/job/task.
	 * This method is called when generating a new application/job/task.
	 * 
	 * @param tablename (application/job/task)
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
	 * Get an application object specified by its id.
	 * This will construct an application object along with all of its jobs,
	 * and all tasks of each of the jobs.
	 * 
	 * @param appId
	 * @return
	 */
	private static Application getApplication(int appId) {
		Application app = null;

		String appName;
		ApplicationType type;
		int priority;
		boolean active;
		boolean complete;

		// Get the application information from the database
		ResultSet rs = dbConn.selectQuery("SELECT * FROM application WHERE id=" + appId + ";");
		try {
			while (rs.next()) {
				// this should only go once since there should only be 1 application with 1 id
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
			System.out.println("[AM] Failed getting an application of id " + appId + ": " + e);
			return null;
		}

		if (!getJobs(app) || app.getJobs().size() < 1) {
			System.out.println("[AM] Failed getting jobs for application of id: " + appId);
			return null;
		}
		
		// add all input files and dependencies (if any).
		for (int jobId: app.getJobs().keySet()) {
			addInputFiles(app.getJob(jobId));
			addDependencies(app.getJob(jobId));
			if (!getTasks(app.getJob(jobId))) {
				System.out.println("[AM] Failed getting tasks for job " + jobId + " of application " + appId);
				return null;
			}
		}

		return app;
	}

	/**
	 * Get a list of {incomplete} applications of a specific type.
	 * 
	 * @param type	Application type
	 * @param complete	
	 * @return
	 */
	private static ArrayList<Application> getApplications(ApplicationType type, boolean complete) {
		ArrayList<Application> result = new ArrayList<Application>();

		ResultSet rs = dbConn.selectQuery("SELECT * FROM application "
				+ "WHERE type='" + type + "' AND active=true AND complete=" + complete + ";");

		int appId;
		String appName;
		int priority;
		boolean active;
		Application app;

		try {
			while (rs.next()) {
				// this should only go once since there should only be 1 application with 1 id
				appId = rs.getInt("id");
				appName = rs.getString("name");
				priority = rs.getInt("priority");
				active = rs.getBoolean("active");

				app = new Application(appId, appName, type, priority);
				app.setComplete(complete);
				app.setActive(active);

				if (!getJobs(app) || app.getJobs().size() < 1) {
					System.out.println("[AM] Failed getting jobs for application of id: " + appId);
					continue;
				}
				
				// add all input files and dependencies (if any).
				for (int jobId: app.getJobs().keySet()) {
					addInputFiles(app.getJob(jobId));
					addDependencies(app.getJob(jobId));
					if (!getTasks(app.getJob(jobId))) {
						System.out.println("[AM] Failed getting tasks for job " + jobId + " of application " + appId);
						return null;
					}
				}

				result.add(app);
			}
		} catch (SQLException e) {
			System.out.println("[AM] Failed getting an applications of type " + type + ": " + e.getMessage());
			return null;
		}
		return result;
	}

	/**
	 * Add the input files of a specific job.
	 * 
	 * @param job
	 */
	private static void addInputFiles(Job job) {
		if (job == null) {
			System.out.println("[AM] Failed getting input files.");
			return;
		}

		ResultSet rs = dbConn.selectQuery("SELECT filename FROM job_file WHERE id=" + job.getId() + ";");
		String filename;
		try {
			while (rs.next()) {
				filename = rs.getString("filename");
				if (filename != null && !filename.isEmpty() && !job.getFileList().contains(filename)) {
					job.addFile(filename);
				}
			}
		} catch (SQLException e) {
			System.out.println("[AM] Failed getting input files for job " + job.getId() + ": " + e.getMessage());
		}
	}

	/**
	 * Add the dependency list of a specific job.
	 * 
	 * @param job
	 */
	private static void addDependencies(Job job) {
		if (job == null) {
			System.out.println("[AM] Failed getting dependencies.");
			return;
		}

		ResultSet rs = dbConn.selectQuery("SELECT dep_id FROM dependency WHERE id=" + job.getId() + ";");
		int depId;
		try {
			while (rs.next()) {
				depId = rs.getInt("dep_id");
				if (depId > -1 && !job.getDependencies().contains(depId)) {
					job.addDependency(depId);
				}
			}
		} catch (SQLException e) {
			System.out.println("[AM] Failed getting dependency list for job " + job.getId() + ": " + e.getMessage());
		}
	}

	/**
	 * Get a list of jobs belonging to a specific application.
	 * 
	 * @param app
	 * @return
	 */
	private static boolean getJobs(Application app) {
		int jobId = -1;
		int priority;
		boolean active;
		boolean complete;

		if (app == null)
			return false;

		JobType jobType;
		String exeFilename;
		// Get all of the applicaiton's job information
		ResultSet rs = dbConn.selectQuery("SELECT id, type, priority, active, complete, exe_filename FROM job WHERE app_id=" + app.getId() + ";");
		try {
			while (rs.next()) {
				jobId = rs.getInt("id");
				jobType = JobType.valueOf(rs.getString("type"));
				priority = rs.getInt("priority");
				active = rs.getBoolean("active");
				complete = rs.getBoolean("complete");
				exeFilename = rs.getString("exe_filename");

				// add the job object to the application
				Job job = new Job(jobId, app.getId(), priority, jobType);
				job.setComplete(complete);
				job.setActive(active);
				job.setExeFile(exeFilename);
				app.addJob(job);
			}
		} catch (SQLException e) {
			return false;
		}
		return true;
	}

	/**
	 * Get a list of tasks belonging to a specific job.
	 * 
	 * @param job
	 * @return
	 */
	private static boolean getTasks(Job job) {
		int taskId;
		String completingNode;
		String status;
		boolean active;
		boolean complete;

		if (job == null)
			return false;

		// Get all tasks for each of the job
		ResultSet rs = dbConn.selectQuery("SELECT id, completing_node, active, complete, status"
				+ " FROM task WHERE job_id =" + job.getId() + ";");
		try {
			while (rs.next()) {
				taskId = rs.getInt("id");
				active = rs.getBoolean("active");
				complete = rs.getBoolean("complete");
				completingNode = rs.getString("completing_node");
				status = rs.getString("status");

				Task task = new Task(taskId, job.getId());
				task.setActive(active);
				task.setComplete(complete);
				task.setCompletedBy(completingNode);
				task.setStatus(status);
				job.addTask(task);
			}
		} catch (SQLException e) {
			return false;
		}
		return true;
	}

	/**
	 * Cancel an application, change the 'active' value of the application into 0 (false).
	 * 
	 * @param request
	 * @return application is successfully canceled 
	 */
	private static boolean cancelApplication(ApplicationRequest request) {
		if (request.getApplicationId() < 0)
			return false;

		String sqlStatement = "UPDATE application SET active = " + 0
				+ " WHERE id =" + request.getApplicationId()
				+ " AND name ='" + request.getApplicationName() + "'"
				+ " AND type ='" + request.getApplicationType() + "';";
		return dbConn.updateQuery(sqlStatement);
	}

	/**
	 * Remove a specific application from the database.
	 * 
	 * @param request
	 * @return 	application is successfully deleted 
	 */
	private static boolean deleteApplication(ApplicationRequest request) {
		if (request.getApplicationId() < 0)
			return false;

		String sqlStatement = "DELETE FROM application "
				+ " WHERE id =" + request.getApplicationId()
				+ " AND name ='" + request.getApplicationName() + "'"
				+ " AND type ='" + request.getApplicationType() + "';";
		return dbConn.updateQuery(sqlStatement);
	}

	/**
	 * Generate a new application into the database.
	 * This will also generate each jobs for the application along with 
	 * the dependency information and tasks for the job.
	 * 
	 * @param request
	 * @return	whether the application was successfully created
	 */
	private static boolean generateApplication(ApplicationRequest request) {
		boolean success = false;
		ArrayList<Job> jobs;
		Job job;
		int id = getNewId("application");

		if (id < 0) {
			id = 1;
		}

		String name = request.getApplicationName();
		ApplicationType applicationType = request.getApplicationType();
		Application application = new Application(id, name, applicationType);

		// Insert the application information
		String sqlStatement = "INSERT INTO application (id, name, priority, active, complete, last_modified, type) " +
				"VALUES (" + id + ", '" + name + "', " + application.getPriority() + ", " + 
				1 + ", " + 0 + ", '" + new Date().toString() + "', '" + applicationType + "');";
		if (!dbConn.updateQuery(sqlStatement)) {
			System.out.println("[AM] Failed creating a new application.");
			return success;
		}

		// Depending of the type of the application, create the corresponding jobs
		System.out.println("[AM] Creating " + applicationType + " jobs.");
		switch (applicationType) {
		case MAPREDUCE:
			jobs = createMapReduceJobs(request, id, application.getPriority());
			if (jobs == null || jobs.size() < 2) {
				System.out.println("[AM] Failed creating a MAPREDUCE jobs.");
				return success;
			}
			application.addJob(jobs.get(0));
			application.addJob(jobs.get(1));
			success = true;
			break;
		case MOBILE:
			job = createMobileJob(request, id, application.getPriority());
			if (job == null) {
				System.out.println("[AM] Failed creating a MOBILE job.");
				return success;
			}
			application.addJob(job);
			success = true;
			break;
		default:
			System.out.println("[AM] Undefined application type.");
			return false;
		}

		int num_nodes = 0;
		for (Job temp: application.getJobs().values()) {
			num_nodes += temp.getNumNodes();
		}

		// update the number of nodes used for running the application
		sqlStatement = "UPDATE application SET num_nodes = " + num_nodes + " WHERE id =" + application.getId() + ";";
		if (!dbConn.updateQuery(sqlStatement)) {
			System.out.println("[AM] Failed updating number of nodes.");
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

		dbConn.updateQuery("INSERT INTO task (id, job_id, active, complete, last_modified) " +
				"VALUES (" + taskId + ", " + job.getId() + ", " + 1 + ", " + 0 + ", '" + new Date().toString() + "');");
		if (filename != null) {
			dbConn.updateQuery("INSERT INTO job_file (id, filename) " + "VALUES (" + job.getId() + ", '" + filename +"');");
		}
		return new Task(taskId, job.getId(), filename, job.getExeFile());
	}

	/**
	 * Create a MOBILE job from an application.
	 * 
	 * @param request
	 * @param applicationId
	 * @param priority
	 * @return
	 */
	private static Job createMobileJob(ApplicationRequest request, int applicationId, int priority) {
		boolean successQuery = false;
		Job job = null;

		// Create a new job object
		int jobId = getNewId("job");
		if (jobId < 0) {
			jobId = 1;
		}
		String exe = request.getJobExecutable(JobType.MOBILE);
		int numNodes = request.getNumWorkers(JobType.MOBILE);

		if (exe == null || numNodes < 1) {
			System.out.println("[MOBILE] Failed creating a MOBILE job.");
			return null;
		}

		job = generateJob(jobId, applicationId, priority, JobType.MOBILE, exe, numNodes);
		// Save the job to the Database
		successQuery = dbConn.updateQuery("INSERT INTO job (id, type, app_id, active, complete, exe_filename, last_modified, num_nodes, priority) " +
				"VALUES (" + jobId + ", '" + JobType.MOBILE + "', " + applicationId + ", " + 1 + ", " + 0 + ", '" +
				exe + "', '" + new Date().toString() + "', " + numNodes + ", " + priority + ");");
		if (!successQuery) {
			System.out.println("[MOBILE] Failed saving a MOBILE job.");
			return null;
		}
		// Create MOBILE tasks
		for (int i = 0; i < numNodes; i++) {
			job.addTask(generateTask(job, null));
		}
		return job;
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
		int numNodes = 0;
		if (mapInputs == null || mapInputs.isEmpty()) {
			System.out.println("[MR] No inputs for MAP.");
			return null;
		}
		if (request.getNumWorkers(JobType.MAP) < 0) {
			numNodes = mapInputs.size();
		} else {
			numNodes = Math.min(request.getNumWorkers(JobType.MAP), mapInputs.size());
		}

		if (mapExe == null || mapInputs == null || mapInputs.size() < 1) {
			System.out.println("[MR] Failed creating a MAP job.");
			return null;
		}
		Job mapJob = generateJob(mapJobId, applicationId, priority, JobType.MAP, mapExe, numNodes);
		// Save the job to the Database
		successQuery = dbConn.updateQuery("INSERT INTO job (id, type, app_id, active, complete, exe_filename, last_modified, num_nodes, priority) " +
				"VALUES (" + mapJobId + ", '" + JobType.MAP + "', " + applicationId + ", " + 1 + ", " + 0 + ", '" +
				mapExe + "', '" + new Date().toString() + "', " + numNodes + ", " + priority + ");");
		if (!successQuery) {
			System.out.println("[MR] Failed saving a MAP job.");
			return null;
		} else {
			System.out.println("[MR] MAP job created.");
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
		if (redInputs == null || redInputs.isEmpty()) {
			System.out.println("[MR] No inputs for REDUCE.");
			return null;
		}
		if (request.getNumWorkers(JobType.REDUCE) < 0) {
			numNodes = redInputs.size();
		} else {
			numNodes = Math.min(request.getNumWorkers(JobType.REDUCE), redInputs.size());
		}

		if (redExe == null || redInputs == null || redInputs.size() < 1) {
			System.out.println("[MR] Failed creating a REDUCE job.");
			return null;
		}
		Job redJob = generateJob(redJobId, applicationId, priority, JobType.REDUCE, redExe, numNodes);
		redJob.addDependency(mapJobId);
		// Save the job to the Database
		successQuery = dbConn.updateQuery("INSERT INTO job (id, type, app_id, active, complete, exe_filename, last_modified, num_nodes, priority) " +
				"VALUES (" + redJobId + ", '" + JobType.REDUCE + "', " + applicationId + ", " + 0 + ", " + 0 + ", '" +
				redExe + "', '" + new Date().toString() + "', " + numNodes + ", " + priority + ");");
		if (!successQuery) {
			System.out.println("[MR] Failed saving a REDUCE job.");
			return null;
		} else {
			System.out.println("[MR] REDUCE job created.");
		}
		successQuery = dbConn.updateQuery("INSERT INTO dependency (id, dep_id) " +
				"VALUES (" + redJobId + ", " + mapJobId + ");");
		if (!successQuery) {
			System.out.println("[MR] Failed saving a dependency record.");
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

	private static class RequestHandlerThread implements Runnable {
		private final Socket clientSock;

		public RequestHandlerThread(Socket sock) {
			clientSock = sock;
		}

		@Override
		public void run() {
			ArrayList<Application> applications = null;
			BufferedReader in = null;
			PrintWriter out = null;

			try {
				in = new BufferedReader(new InputStreamReader(clientSock.getInputStream()));
				out = new PrintWriter(clientSock.getOutputStream());
				String input = in.readLine();

				ApplicationRequest appRequest = gson.fromJson(input, ApplicationRequest.class);
				TaskRequest taskRequest = gson.fromJson(input, TaskRequest.class);
				ScheduleRequest scheduleRequest = gson.fromJson(input, ScheduleRequest.class);
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
					case GETINCOMPLETEAPP:
						applications = getApplications(appRequest.getApplicationType(), false);
						out.println(gson.toJson(applications));
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
						out.println(gson.toJson(false));
					}
				} 
				// Task request handler
				else if (taskRequest != null && taskRequest.getType() != null && taskRequest.getNodeId() != null) {
					System.out.println("[AM] Received a task request: " + taskRequest.getType());
					Task task = null;
					boolean removed = false;

					switch (taskRequest.getType()) {
					case GET:	// get a task that is scheduled for the node, if any.
						synchronized(scheduleLock) {
							task = schedule.getFirstTask(taskRequest.getNodeId());
						}
						if (task != null) {
							System.out.println("[AM] TASK: " + task.getId() + " type:" + task.getType().toString());
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
						if (taskRequest.getTaskId() >= 0 && taskRequest.getNodeId() != null) {				
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
					case CHECK: // check the status of a task
						if (taskRequest.getTaskId() >= 0) {
							String sqlStatement = "SELECT * FROM task WHERE id=" + taskRequest.getTaskId() + ";";
							ResultSet rs = dbConn.selectQuery(sqlStatement);
							try {
								task = new Task(rs.getInt("id"), rs.getInt("job_id"));
								task.setActive(rs.getBoolean("active"));
								task.setComplete(rs.getBoolean("complete"));
							} catch (SQLException e) {
								System.out.println("[AM] Failed getting task information for checking: " + e);
							}
							out.println(gson.toJson(task));
						}
						break;
					default:
						System.out.println("[AM] Invalid request: " + taskRequest.getType());
						out.println(gson.toJson(false));
					}
				} 
				// Schedule request handler
				else if (scheduleRequest != null && scheduleRequest.getNodeTask() != null && !scheduleRequest.getNodeTask().isEmpty()) {
					System.out.println("[AM] Received a schedule request.");
					synchronized(scheduleLock) {
						for (String nodeId: scheduleRequest.getNodeTask().keySet()) {
							schedule.addTask(nodeId, scheduleRequest.getNodeTask().get(nodeId));
						}
					}
					out.println(gson.toJson(true));
				} else {
					System.out.println("[AM] Invalid request. ");
					out.println(gson.toJson(false));
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
