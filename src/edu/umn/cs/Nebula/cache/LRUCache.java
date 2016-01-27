package edu.umn.cs.Nebula.cache;

import java.util.LinkedHashMap;
import java.util.Map;

import edu.umn.cs.Nebula.model.ArticleKey;

public class LRUCache extends LinkedHashMap<ArticleKey,String> {

	private static final long serialVersionUID = 1L;
	private final int maxSize;

	/**
	 * Create a new resource cache.
	 * @param maxSize The maximum number of entries that may occupy the cache at a time.
	 */
	public LRUCache(int maxSize) {
		super(maxSize + 1, 0.75f, true);
	    this.maxSize = maxSize;
	}
	
	public int getMaxSize() {
		return maxSize;
	}

	protected boolean removeEldestEntry(Map.Entry<ArticleKey,String> eldest) {
		return size() > maxSize;
	}
}
