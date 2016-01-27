package edu.umn.cs.Nebula.request;

import java.util.HashSet;
import java.util.Set;

import edu.umn.cs.Nebula.model.ArticleKey;

public class MobileRequest {
	private MobileRequestType type;
	private String userId;
	private String userIp;
	private double latitude;
	private double longitude;
	private Set<ArticleKey> keys;
	private String content;
	
	public MobileRequest(MobileRequestType type, String userId) {
		this.type = type;
		this.userId = userId;
	}
	
	public MobileRequest(MobileRequestType type, String userId, String userIp) {
		this.type = type;
		this.userId = userId;
		this.userIp = userIp;
	}
	
	public MobileRequestType getType() {
		return type;
	}
	
	public void setType(MobileRequestType type) {
		this.type = type;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getUserIp() {
		return userIp;
	}

	public void setUserIp(String userIp) {
		this.userIp = userIp;
	}
	
	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public Set<ArticleKey> getKeys() {
		return keys;
	}

	public void setKeys(Set<ArticleKey> keys) {
		this.keys = keys;
	}
	
	public void addKey(ArticleKey key) {
		if (keys == null) {
			keys = new HashSet<ArticleKey>();
		}
		keys.add(key);
	}
}
