package edu.umn.cs.Nebula.mobile;

import java.util.Comparator;

public class ArticleKey implements Comparator<ArticleKey> {

	private final ArticleTopic topic;
	private final String title;
	private final String url;
	
	private double score = -1;

	public ArticleKey(ArticleTopic topic, String title, String url) {
		this.topic = topic;
		this.title = title;
		this.url = url;
	}

	public ArticleTopic getTopic() {
		return topic;
	}

	public String getTitle() {
		return title;
	}

	public String getUrl() {
		return url;
	}

	@Override
	public String toString() {
		return url;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof ArticleKey) {
			ArticleKey other = (ArticleKey) o;

			if (topic == null && other.topic != null) {
				return false;
			} else if (topic != null && other.topic == null) {
				return false;
			} else if (topic != null && other.topic != null && !topic.equals(other.topic)) {
				return false;
			}

			if (title == null && other.title != null) {
				return false;
			} else if (title != null && other.title == null) {
				return false;
			} else if (title != null && other.title != null && !title.equals(other.title)) {
				return false;
			}

			if (url == null && other.url != null) {
				return false;
			} else if (url != null && other.url == null) {
				return false;
			} else if (url != null && other.url != null && !url.equals(other.url)) {
				return false;
			}

			return true;

		} else {
			return false;
		}
	}
	
	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	@Override
	public int hashCode() {
		int hash = 1;
		if (topic != null) {
			hash *= topic.hashCode();
		}
		if (title != null) {
			hash *= title.hashCode();
		}
		if (url != null) {
			hash *= url.hashCode();
		}
		return hash;
	}

	@Override
	public int compare(ArticleKey key1, ArticleKey key2) {
		return (int) (key2.getScore() - key1.getScore());
	}
}