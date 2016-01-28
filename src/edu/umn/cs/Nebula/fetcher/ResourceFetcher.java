package edu.umn.cs.Nebula.fetcher;

import java.util.HashMap;
import java.util.Set;

import edu.umn.cs.Nebula.mobile.ArticleKey;
import edu.umn.cs.Nebula.mobile.ArticleTopic;

public interface ResourceFetcher {

	/**
	 * Get the content of data with the specified key from the origin.
	 * @param key
	 * @return
	 */
	public String fetchFromOrigin(ArticleKey key);

	/**
	 * Get all available resource keys from the origin without downloading the contents.
	 * @return
	 */
	public HashMap<ArticleTopic, Set<ArticleKey>> getAvailableResources();
	
	
}
