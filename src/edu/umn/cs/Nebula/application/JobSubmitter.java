package edu.umn.cs.Nebula.application;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import com.google.gson.Gson;

import edu.umn.cs.Nebula.job.Job;
import edu.umn.cs.Nebula.job.JobType;
import edu.umn.cs.Nebula.request.JobRequest;
import edu.umn.cs.Nebula.request.JobRequestType;

public class JobSubmitter {
	private final static String jobManager = "134.84.121.87";
	private final static int jobSubmissionPort = 6411;
	private final static Gson gson = new Gson();

	public static void main(String[] args) throws IOException, InterruptedException {		
		// create a job and a job request
		Job job = new Job(
				0,
				0,
				JobType.MOBILE, 
				"java -jar", 
				"TestApp.jar",
				null);
		JobRequest request = new JobRequest(job, JobRequestType.SUBMIT);
		
		// submit the job to the jobManager and wait for the reply
		Socket socket = new Socket(jobManager, jobSubmissionPort);
		PrintWriter out = new PrintWriter(socket.getOutputStream());
		BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		out.println(gson.toJson(request));
		out.flush();
		System.out.println(in.readLine());
		out.close();
		in.close();
		socket.close();
	}

}
