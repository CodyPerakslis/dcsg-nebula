package edu.umn.cs.Nebula.mobile;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import edu.umn.cs.Nebula.model.LRUCache;
import edu.umn.cs.Nebula.node.NodeInfo;
import edu.umn.cs.Nebula.node.NodeType;

public class NewsFetcherApplication {
	private static HashMap<String, NodeInfo> mobileUsers = new HashMap<String, NodeInfo>();

	private static long lastUpdate = 0;
	private static HashMap<ArticleTopic, Set<ArticleKey>> resourceKeys = new HashMap<ArticleTopic, Set<ArticleKey>>();
	private static Object resourceKeysLock = new Object();
	private static int maxCachedArticles = 200;
	private static LRUCache<ArticleKey> articleContentMap = new LRUCache<ArticleKey>(maxCachedArticles);

	private static CNNFetcher resourceFetcher = new CNNFetcher();

	/**
	 * Setup threads that periodically update resources from the resource provider
	 */
	public static void setupResourceUpdate() {
		System.out.println("[NEWS] Connecting to Resources Content");
		Thread resourceUpdateThread = new Thread(new ResourceUpdateThread());
		resourceUpdateThread.start();
	}

	/**
	 * Update the location information of the mobile user.
	 */
	public static boolean updateLocation(MobileRequest request) {
		String userId = request.getUserId();
		double lat = request.getLatitude();
		double lon = request.getLongitude();
		NodeInfo user = mobileUsers.get(userId);

		if (lat == Double.MIN_VALUE || lon == Double.MIN_VALUE) {
			return false;
		}

		if (user == null) {
			user = new NodeInfo(userId, userId, lat, lon, NodeType.MOBILE);
		} else {
			user.setLatitude(lat);
			user.setLongitude(lon);
		}

		mobileUsers.put(userId, user);
		return true;
	}

	/**
	 * Update resources request from the clients.
	 */
	public static void survey() {
		synchronized (resourceKeysLock) {
			resourceKeys.clear();
			resourceKeys = resourceFetcher.getAvailableResources();
			System.out.println("[NEWS] Number of available resources: " + resourceKeys.size());
			lastUpdate = System.currentTimeMillis() / 1000;
		}
	}

	public static HashMap<String, String> fetch(Set<ArticleKey> request) {
		HashMap<String, String> contents = new HashMap<String, String>();

		for (ArticleKey key: request) {
			if (articleContentMap.containsKey(key)) {
				contents.put(key.getTitle(), (String) articleContentMap.get(key));
			} else {
				// the article is not cached, fetch it from the origin and cache it
				String content = resourceFetcher.fetchFromOrigin(key);
				contents.put(key.getTitle(), content);
				articleContentMap.add(key, content);
				if (!resourceKeys.containsKey(key.getTopic())) {
					resourceKeys.put(key.getTopic(), new HashSet<ArticleKey>());
				}
				resourceKeys.get(key.getTopic()).add(key);
			}
		}
		return contents;
	}

	public static void cache(Set<ArticleKey> request) {
		// cache the contents of each key in the cache
		for (ArticleKey key: request) {
			String content = resourceFetcher.fetchFromOrigin(key);
			if (content == null)
				continue;
			articleContentMap.add(key, content);
			if (!resourceKeys.containsKey(key.getTopic())) {
				resourceKeys.put(key.getTopic(), new HashSet<ArticleKey>());
			}
			resourceKeys.get(key.getTopic()).add(key);
		}
	}

	/**
	 * A thread that periodically updates the list of available resources
	 */
	private static class ResourceUpdateThread implements Runnable {
		private long updateInterval = 10 * 60 * 1000;
		private long currentTime = System.currentTimeMillis();

		@Override
		public void run() {
			while (true) {
				currentTime = System.currentTimeMillis();

				synchronized (resourceKeysLock) {
					if (currentTime - lastUpdate >= updateInterval) {
						System.out.println("[NEWS] Updating resources...");
						resourceKeys.clear();
						resourceKeys = resourceFetcher.getAvailableResources();
						System.out.println("[NEWS] Number of available resources: " + resourceKeys.size());
						lastUpdate = currentTime;
					} else {
						System.out.println(currentTime - lastUpdate + " < " + updateInterval);
					}
				}
				try {
					Thread.sleep(updateInterval);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

		}
	}
}
