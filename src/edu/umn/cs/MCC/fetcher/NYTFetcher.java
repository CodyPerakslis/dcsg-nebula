package edu.umn.cs.MCC.fetcher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import edu.umn.cs.MCC.model.ArticleKey;
import edu.umn.cs.MCC.model.ArticleTopic;

public class NYTFetcher implements ResourceFetcher {
	private HashMap<ArticleTopic, String> rssUrls;

	public NYTFetcher() {
		rssUrls = new HashMap<ArticleTopic, String>();
		rssUrls.put(ArticleTopic.POLITICS, "http://feeds.abcnews.com/abcnews/politicsheadlines");
		rssUrls.put(ArticleTopic.BUSINESS, "http://feeds.abcnews.com/abcnews/moneyheadlines");
		rssUrls.put(ArticleTopic.TECHNOLOGY, "http://feeds.abcnews.com/abcnews/technologyheadlines");
		rssUrls.put(ArticleTopic.ENTERTAINMENT, "http://feeds.abcnews.com/abcnews/entertainmentheadlines");
		rssUrls.put(ArticleTopic.HEALTH, "http://feeds.abcnews.com/abcnews/healthheadlines");
	}

	@Override
	public String fetchFromOrigin(ArticleKey key) {
		String result = "";

		try {			
			// connect to the NYT server origin to get the content associated with the key
			URL url = new URL(key.getUrl());
			URLConnection conn = url.openConnection();
			
			BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			String inputLine;

			// get the content
			String content = "";
			while ((inputLine = in.readLine()) != null) {
				content += inputLine;
			}
			// parse the content, we only care about the body of the content
			Document doc = Jsoup.parse(content);
			System.out.println(content);
			
			Elements paragraphs = doc.select("p.story-body-text story-content");
			// the article does not have any paragraph
			if (paragraphs == null || paragraphs.isEmpty()) {
				return null;
			}
			
			for (Element p: paragraphs) {
				result += p.text() + "\n";

				if (!p.text().endsWith("\n"))
					result += "\n";
			}
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return result;
	}

	@Override
	public HashMap<ArticleTopic, Set<ArticleKey>> getAvailableResources() {
		HashMap<ArticleTopic, Set<ArticleKey>> result = new HashMap<ArticleTopic, Set<ArticleKey>>();

		for (ArticleTopic topic: rssUrls.keySet()) {
			result.put(topic, getArticles(rssUrls.get(topic)));
		}
		return result;
	}

	/**
	 * Get a list of ArticleKey from the RSS URL.
	 * 
	 * @param urlAddress
	 * @return
	 */
	public Set<ArticleKey> getArticles(String urlAddress) {
		URL rssUrl;
		BufferedReader in = null;
		ArticleTopic topic;
		Set<ArticleKey> result = new HashSet<ArticleKey>();
		String[] urlPart = urlAddress.split("/");

		if (urlPart[urlPart.length-1].equalsIgnoreCase("politicsheadlines")) {
			topic = ArticleTopic.POLITICS;
		} else if (urlPart[urlPart.length-1].equalsIgnoreCase("moneyheadlines")) {
			topic = ArticleTopic.BUSINESS;
		} else if (urlPart[urlPart.length-1].equalsIgnoreCase("technologyheadlines")) {
			topic = ArticleTopic.TECHNOLOGY;
		} else if (urlPart[urlPart.length-1].equalsIgnoreCase("entertainmentheadlines")) {
			topic = ArticleTopic.ENTERTAINMENT;
		} else if (urlPart[urlPart.length-1].equalsIgnoreCase("healthheadlines")) {
			topic = ArticleTopic.HEALTH;
		} else {
			System.out.println("Unable to find the topic from the RSS: " + urlAddress);
			topic = null;
		}

		try {
			rssUrl = new URL(urlAddress);
			in = new BufferedReader(new InputStreamReader(rssUrl.openStream()));
			String line;

			while ((line = in.readLine()) != null) {
				int titleEndIndex = 0;
				int titleStartIndex = 0;
				int linkStratIndex = 0;
				int linkEndIndex = 0;
				String title = null;
				String link = null;
				while (titleStartIndex >= 0) {
					title = null;
					titleStartIndex = line.indexOf("<title>", titleEndIndex);
					if (titleStartIndex >= 0) {
						titleEndIndex = line.indexOf("</title>", titleStartIndex);
						title = line.substring(titleStartIndex + "<title>".length(), titleEndIndex);
					}
					
					if (title != null && !title.startsWith("ABC News:")) {
						System.out.println(title);
						linkStratIndex = line.indexOf("<link>", titleEndIndex);
						linkEndIndex = line.indexOf("</link>", titleStartIndex);
						link = line.substring(linkStratIndex + "<link>".length(), linkEndIndex);
						
						if (link != null && !link.isEmpty()) {
							ArticleKey key = new ArticleKey(topic, title, link);
							result.add(key);
						}
					}
				}
			}
			in.close();
		} catch (IOException e) {
			System.out.println(e);
			return result;
		}

		return result;
	}
	
	public static void main(String args[]) {
		NYTFetcher fetcher = new NYTFetcher();
		HashMap<ArticleTopic, Set<ArticleKey>> temp = fetcher.getAvailableResources();
		
		// ArrayList<ArticleKey> test = new ArrayList<ArticleKey>(temp.get(ArticleTopic.POLITICS));
	}
}
