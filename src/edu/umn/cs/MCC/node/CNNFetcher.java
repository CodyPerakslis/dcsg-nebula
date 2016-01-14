package edu.umn.cs.MCC.node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
	
	private String[] rssUrls = {
			"http://rss.cnn.com/rss/cnn_topstories.rss", 
			"http://rss.cnn.com/rss/cnn_world.rss", 
			"http://rss.cnn.com/rss/cnn_us.rss", 
			"http://rss.cnn.com/rss/cnn_allpolitics.rss", 
			"http://rss.cnn.com/rss/cnn_tech.rss", 
			"http://rss.cnn.com/rss/cnn_showbiz.rss"};
	
	/**
	 * Get the content associated with the provided key.
	 * 
	 * @param key
	 * @return article content
	 */
    public String fetchFromOrigin(ArticleKey key) {
    	String result = "";
    	
        try {
        	URL url = new URL(key.getUrl());
            URLConnection conn = url.openConnection();
            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String inputLine;
            
            // get the content
            String content = "";
            while ((inputLine = in.readLine()) != null) {
            	content += inputLine;
            }
            
            Document doc = Jsoup.parse(content);
			Elements paragraphs = doc.select("p.zn-body__paragraph");
			
			if (paragraphs == null || paragraphs.isEmpty()) {
				return null;
			}
			
			for (Element p: paragraphs) {
				result += p.text();
			}
			
			if (result == null || result.isEmpty()) {
				return null;
			}
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
    	return result;
    }
    
    /**
     * Get a list of available Article keys
     * 
     * @return
     */
    public Set<ArticleKey> getAvailableResources() {
    	Set<ArticleKey> articleKeys = new HashSet<ArticleKey>();
    	
    	for (String rssUrl: rssUrls) {
    		articleKeys.addAll(getArticles(rssUrl));
    	}
        return articleKeys;
    }
	
	/**
	 * Get a list of ArticleKey from the RSS url.
	 * 
	 * @param urlAddress
	 * @return
	 */
	public List<ArticleKey> getArticles(String urlAddress) {
		URL rssUrl;
		BufferedReader in = null;
		ArticleTopic topic;
		List<ArticleKey> result = new ArrayList<ArticleKey>();
		String[] urlPart = urlAddress.split("/");
		
		if (urlPart[urlPart.length-1].equalsIgnoreCase("cnn_topstories.rss") ||
				urlPart[urlPart.length-1].equalsIgnoreCase("cnn_world.rss") ||
				urlPart[urlPart.length-1].equalsIgnoreCase("cnn_us.rss")) {
			topic = ArticleTopic.NEWS;
		} else if (urlPart[urlPart.length-1].equalsIgnoreCase("cnn_allpolitics.rss")) {
			topic = ArticleTopic.POLITICS;
		} else if (urlPart[urlPart.length-1].equalsIgnoreCase("cnn_showbiz.rss")) {
			topic = ArticleTopic.ENTERTAINMENT;
		} else if (urlPart[urlPart.length-1].equalsIgnoreCase("cnn_tech.rss")) {
			topic = ArticleTopic.TECHNOLOGY;
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