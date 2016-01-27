package edu.umn.cs.Nebula.node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import edu.umn.cs.Nebula.cache.LRUCache;
import edu.umn.cs.Nebula.fetcher.CNNFetcher;
import edu.umn.cs.Nebula.model.ArticleKey;
import edu.umn.cs.Nebula.model.ArticleTopic;
import edu.umn.cs.Nebula.request.MobileRequest;
import edu.umn.cs.Nebula.request.MobileRequestType;

public class PrimaryNode {

	private static final int port = 6430;
	private static final int poolSize = 10;
	private static HashMap<String, NodeInfo> mobileUsers = new HashMap<String, NodeInfo>();

	private static HashMap<String, NodeInfo> nodes = new HashMap<String, NodeInfo>();
	private static Object nodesLock = new Object();
	private static long updateFrequency = 10000;
	private static String cloudServer = "http://localhost:8080/MCCInterface/WebInterface";

	private static long lastUpdate = 0;
	private static final int maxArticlesPerTopic = 20;
	private static HashMap<ArticleTopic, Set<ArticleKey>> newArticles = new HashMap<ArticleTopic, Set<ArticleKey>>();
	private static Object resourceKeysLock = new Object();
	private static final int cacheSize = 100;
	private static LRUCache articleContentMap = new LRUCache(cacheSize);

	private static CNNFetcher resourceFetcher = new CNNFetcher();
	private static String id = null;


	public static void main(String[] args) {
		// Connecting to Cloud Server
		System.out.println("Connecting to Cloud Server");
		Thread cloudServerThread = new Thread(new CloudServerThread());
		cloudServerThread.start();

		// Setup threads that periodically update resources from the resource provider
		System.out.println("Connecting to Resources Content");
		Thread resourceUpdateThread = new Thread(new ResourceUpdateThread());
		resourceUpdateThread.start();

		// Listening for client requests
		ExecutorService requestPool = Executors.newFixedThreadPool(poolSize);
		ServerSocket serverSock = null;
		try {
			serverSock = new ServerSocket(port);
			System.out.println("Listening for client requests on port " + port);
			while (true) {
				requestPool.submit(new RequestHanlerThread(serverSock.accept()));
			}
		} catch (IOException e) {
			System.err.println("Failed to establish listening socket: " + e);
		} finally {
			requestPool.shutdown();
			if (serverSock != null) {
				try {
					serverSock.close();
				} catch (IOException e) {
					System.err.println("Failed to close listening socket");
				}
			}
		}
	}

	/**
	 * Update the location of a mobile user.
	 */
	private static boolean updateLocation(MobileRequest request) {
		String userId = request.getUserId();
		double lat = request.getLatitude();
		double lon = request.getLongitude();
		NodeInfo user = mobileUsers.get(userId);

		// make sure the coordinate is within the correct range
		if (lat < -90 || lat > 90 || lon < -180 || lon > 180) {
			return false;
		}

		if (user == null) {
			// new user
			user = new NodeInfo(userId, userId, lat, lon, NodeType.MOBILE);
		} else {
			// update the coordinate of the user
			user.setLatitude(lat);
			user.setLongitude(lon);
		}

		mobileUsers.put(userId, user);
		return true;
	}

	/**
	 * Update resources from the origin.
	 */
	private static void survey() {
		synchronized (resourceKeysLock) {
			HashMap<ArticleTopic, Set<ArticleKey>> newResources = resourceFetcher.getAvailableResources();
			
			if (newResources == null) {
				System.out.println("No new articles found.");
				return;
			}
			
			for (ArticleTopic topic: newResources.keySet()) {
				if (!newArticles.containsKey(topic)) {
					newArticles.put(topic, new HashSet<ArticleKey>());
				}
				newArticles.get(topic).addAll(newResources.get(topic));
				if (newArticles.get(topic).size() > maxArticlesPerTopic) {
					newArticles.get(topic).clear();
					newArticles.get(topic).addAll(newResources.get(topic));
				}
			}
			lastUpdate = System.currentTimeMillis() / 1000;
		}
	}

	/**
	 * Get the content associated with the provided key from one of the peer node.
	 * 
	 * @param key
	 * @return article content
	 */
	@SuppressWarnings("resource")
	public static String fetchFromPeer(ArticleKey key) {
		MobileRequest request = null;
		Socket peerSocket = null;
		BufferedReader in = null;
		PrintWriter out = null;
		Gson gson = new Gson();
		String content = null;

		if (key == null)
			return null;

		try {
			// use the "GET" request to fetch data from another primary node
			request = new MobileRequest(MobileRequestType.GET, null);
			peerSocket = new Socket(key.getUrl(), port);

			in = new BufferedReader(new InputStreamReader(peerSocket.getInputStream()));
			out = new PrintWriter(peerSocket.getOutputStream());

			// send the request
			out.println(gson.toJson(request));
			out.flush();
			// read the content
			content = gson.fromJson(in.readLine(), String.class);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (in != null) in.close();
				if (out != null) out.close();	
			} catch (IOException e) {
				System.out.println("Failed closing reader or writer!");
			}	
		}

		return content;
	}

	/**
	 * A thread that periodically updates the list of available resources
	 * @author albert
	 */
	private static class ResourceUpdateThread implements Runnable {
		private long updateInterval = 600000; // 10 minutes
		private long currentTime = System.currentTimeMillis();

		@Override
		public void run() {
			while (true) {
				currentTime = System.currentTimeMillis();

				// check whether needs to update the resources or not
				if (currentTime - lastUpdate < updateInterval) {
					System.out.println(currentTime - lastUpdate + " < " + updateInterval);
					return;
				}
				
				survey();
				try {
					Thread.sleep(updateInterval);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

		}
	}

	/**
	 * A thread that periodically gets updates on available primary nodes from the Cloud Server
	 * @author albert
	 */
	private static class CloudServerThread implements Runnable {

		@Override
		public void run() {
			System.out.println("Connecting to " + cloudServer);
			
			while (true) {
				PrintWriter out = null;
				BufferedReader in = null;
				Gson gson = new Gson();

				try {
					// connecting to Cloud Server
					URL url = new URL(cloudServer);
					HttpURLConnection conn = (HttpURLConnection) url.openConnection();
					conn.setRequestMethod("POST");
					conn.setDoInput(true);
					conn.setDoOutput(true);

					out = new PrintWriter(conn.getOutputStream());
					if (id == null) {
						out.write("id=" + id + "&type=NEW&nodeType=PRIMARY");
					} else {
						out.write("id=" + id + "&type=ONLINE&nodeType=PRIMARY");
					}
					out.flush();
					conn.connect();
					in = new BufferedReader(new InputStreamReader(conn.getInputStream()));

					if (id == null) {
						HashMap<String, NodeInfo> onlineNodes = gson.fromJson(in.readLine(), new TypeToken<HashMap<String, NodeInfo>>(){}.getType());
						if (onlineNodes != null && !onlineNodes.isEmpty()) {
							String[] temp = onlineNodes.keySet().toArray(new String[onlineNodes.keySet().size()]);
							id = temp[0];
						}
					} else {
						synchronized (nodesLock) {
							HashMap<String, NodeInfo> onlineNodes = gson.fromJson(in.readLine(), new TypeToken<HashMap<String, NodeInfo>>(){}.getType());

							if (onlineNodes == null || onlineNodes.isEmpty()) {
								nodes.clear();
								System.out.println("No online nodes.");
							} else {
								nodes = onlineNodes;
							}
						}
					}
					in.close();
				} catch (IOException e) {
					System.out.println("Failed getting primary nodes from Cloud Server: " + e);
					e.printStackTrace();
				}
				try {
					Thread.sleep(updateFrequency);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * A thread that handles requests from clients.
	 * @author albert
	 */
	private static class RequestHanlerThread implements Runnable {
		private final Socket clientSock;

		public RequestHanlerThread(Socket sock) {
			clientSock = sock;
		}

		@Override
		public void run() {
			BufferedReader in = null;
			PrintWriter out = null;
			Gson gson = new Gson();
			HashMap<String, String> contents = new HashMap<String, String>();
			String content = null;

			try {
				in = new BufferedReader(new InputStreamReader (clientSock.getInputStream()));
				out = new PrintWriter(clientSock.getOutputStream());

				MobileRequest request = gson.fromJson(in.readLine(), new TypeToken<MobileRequest>(){}.getType());
				System.out.print("Received a Mobile Request: " + request.getType() + ". Processing ...");

				switch (request.getType()) {
				case LOCATION:
					boolean success = updateLocation(request);
					out.println(gson.toJson(success));
					break;
				case SURVEY:
					survey();
					out.println(gson.toJson(articleContentMap.keySet()));
					break;
				case CACHE:					
					for (ArticleKey key: request.getKeys()) {
						if (articleContentMap.containsKey(key)) {
							continue;
						}
						// the article is not cached, fetch it from the origin and cache it
						content = resourceFetcher.fetchFromOrigin(key);
						contents.put(key.getTitle(), content);
						articleContentMap.put(key, content);

					}
					// reply with a set of article keys whose contents are cached
					out.println(gson.toJson(articleContentMap.keySet()));
					break;
				case CACHEPEER:
					// cache the contents from one of the peer nodes
					for (ArticleKey key: request.getKeys()) {
						if (articleContentMap.containsKey(key)) {
							continue;
						}
						content = fetchFromPeer(key);
						if (content == null)
							continue;
						articleContentMap.put(key, content);
					}
					// reply with a set of article keys whose contents are cached
					out.println(gson.toJson(articleContentMap.keySet()));
					break;
				case FETCH:
					for (ArticleKey key: request.getKeys()) {
						if (articleContentMap.containsKey(key)) {
							contents.put(key.getTitle(), articleContentMap.get(key));
						} else {
							// the article is not cached, fetch it from the origin and cache it
							content = resourceFetcher.fetchFromOrigin(key);
							contents.put(key.getTitle(), content);
							articleContentMap.put(key, content);
						}
					}
					out.println(gson.toJson(contents));
					break;
				case FETCHTOPIC:
					List<ArticleKey> keys = new ArrayList<ArticleKey>(request.getKeys());
					int n = keys.size();
					ArticleTopic topic = null;
					int i = 0;
					
					if (n < 1 || (topic = keys.get(0).getTopic()) == null) {
						System.out.println("Invalid input. Topic: " + topic);
						contents.put("ERROR", "Invalid input. Topic: " + topic);
						out.println(gson.toJson(contents));
						break;
					}

					for (ArticleKey key: newArticles.get(topic)) {
						if (i >= n)
							break;

						if (articleContentMap.containsKey(key)) {
							contents.put(key.getTitle(), articleContentMap.get(key));
						} else {
							// the article is not cached, fetch it from the origin and cache it
							content = resourceFetcher.fetchFromOrigin(key);
							contents.put(key.getTitle(), content);
							articleContentMap.put(key, content);
						}
						i++;
					}
					out.println(gson.toJson(contents));
					break;
				case GET:
					// this request is a download request from a peer. 
					if (request.getKeys() != null && !request.getKeys().isEmpty()) {
						// there should only be 1 key
						for (ArticleKey key: request.getKeys()) {
							if (!articleContentMap.containsKey(key))
								continue;
							content = articleContentMap.get(key);
						}
					}
					out.println(gson.toJson(content));
					break;
				default:
					System.out.println("Undefined request type: " + request.getType());
					break;
				}
				System.out.println("[OK]");
				out.flush();
			} catch (IOException e) {
				System.err.println("Error: " + e);
			} finally {
				try {
					if (in != null) in.close();
					if (out != null) out.close();
					if (clientSock != null) clientSock.close();
				} catch (IOException e) {
					System.out.println("Failed to close streams or socket: " + e);
				}
			}
		}
	}
}
