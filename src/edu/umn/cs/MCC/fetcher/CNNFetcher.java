package edu.umn.cs.MCC.fetcher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import edu.umn.cs.MCC.model.ArticleKey;
import edu.umn.cs.MCC.model.ArticleTopic;

/**
 * @author albert
 */
public class CNNFetcher implements ResourceFetcher {
	private HashMap<ArticleTopic, String> rssUrls;
	
	public CNNFetcher() {
		rssUrls = new HashMap<ArticleTopic, String>();
		rssUrls.put(ArticleTopic.POLITICS, "http://rss.cnn.com/rss/cnn_allpolitics.rss");
		rssUrls.put(ArticleTopic.BUSINESS, "http://rss.cnn.com/rss/money_latest.rss");
		rssUrls.put(ArticleTopic.TECHNOLOGY, "http://rss.cnn.com/rss/cnn_tech.rss");
		rssUrls.put(ArticleTopic.ENTERTAINMENT, "http://rss.cnn.com/rss/cnn_showbiz.rss");
		rssUrls.put(ArticleTopic.HEALTH, "http://rss.cnn.com/rss/cnn_health.rss");
	}
	
	/**
	 * Get the content associated with the provided key.
	 * 
	 * @param key
	 * @return article content
	 */
    public String fetchFromOrigin(ArticleKey key) {
    	String result = "";
    	
        try {
        	// connect to the CNN server origin to get the content of the key
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
			Elements paragraphs = doc.select("p.zn-body__paragraph");
			
			// the article does not have any paragraph
			if (paragraphs == null || paragraphs.isEmpty()) {
				return null;
			}
			
			for (Element p: paragraphs) {
				result += p.text();
			}
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
    	return result;
    }
    
    /**
     * Get a set of available Article keys
     * @return
     */
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
		
		if (urlPart[urlPart.length-1].equalsIgnoreCase("cnn_allpolitics.rss")) {
			topic = ArticleTopic.POLITICS;
		} else if (urlPart[urlPart.length-1].equalsIgnoreCase("money_latest.rss")) {
			topic = ArticleTopic.BUSINESS;
		} else if (urlPart[urlPart.length-1].equalsIgnoreCase("cnn_tech.rss")) {
			topic = ArticleTopic.TECHNOLOGY;
		} else if (urlPart[urlPart.length-1].equalsIgnoreCase("cnn_showbiz.rss")) {
			topic = ArticleTopic.ENTERTAINMENT;
		} else if (urlPart[urlPart.length-1].equalsIgnoreCase("cnn_health.rss")) {
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
			        if (title != null) {
			        	linkStratIndex = line.indexOf("<link>", titleEndIndex);
			        	linkEndIndex = line.indexOf("</link>", titleStartIndex);
			        	link = line.substring(linkStratIndex + "<link>".length(), linkEndIndex);
			        }
			        if (title != null && !title.isEmpty() && link != null && !link.isEmpty()) {
			        	ArticleKey key = new ArticleKey(topic, title, link);
			        	if (title.startsWith("CNN.com")) {
			        		continue;
			        	}
			        	result.add(key);
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
}