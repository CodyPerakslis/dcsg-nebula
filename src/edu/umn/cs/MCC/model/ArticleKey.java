package edu.umn.cs.MCC.model;

public class ArticleKey {

	private final ArticleTopic topic;
	private final String title;
	private final String url;

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
}
