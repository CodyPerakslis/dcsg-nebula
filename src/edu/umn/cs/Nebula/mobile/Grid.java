package edu.umn.cs.Nebula.mobile;

import java.util.HashMap;
import java.util.LinkedList;

public class Grid {
	private final int k;
	private final double minLatitude, maxLatitude, minLongitude, maxLongitude;
	
	private HashMap<String, GridCell> cells;
	
	public Grid(int k, double minLatitude, double maxLatitude, double minLongitude, double maxLongitude) {
		this.k = k;
		this.minLatitude = minLatitude;
		this.maxLatitude = maxLatitude;
		this.minLongitude = minLongitude;
		this.maxLongitude = maxLongitude;
		
		cells = new HashMap<String, GridCell>();
		String id;
		for (int i = 0; i < k; i++) {
			id = Integer.toString(i) + "_";
			for (int j = 0; j < k; j++) {
				cells.put(id + Integer.toString(j), new GridCell(id + Integer.toString(j)));
			}
		}
	}
	
	public boolean validCoordinate(double latitude, double longitude) {
		if (latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180 || 
				latitude < minLatitude || latitude > maxLatitude || longitude < minLongitude || longitude > maxLongitude)
			return false;
		return true;
	}

	public int getK() {
		return k;
	}
	
	public String getGridLocation(double latitude, double longitude) {
		double relativeLat;
		double relativeLon;
		
		if (!validCoordinate(latitude, longitude))
			return null;
		
		if (minLatitude < 0) {
			relativeLat = latitude + Math.abs(minLatitude);
		} else {
			relativeLat = latitude - minLatitude;
		}
		if (minLongitude < 0) {
			relativeLon = longitude + Math.abs(minLongitude);
		} else {
			relativeLon = longitude - minLongitude;
		}
		
		double cellWidth = (maxLongitude - minLongitude) / k;
		double cellHeight = (maxLatitude - minLatitude) / k;
		
		int latitudeLocation = (int) Math.floor(relativeLat / cellHeight);
		int longitudeLocation = (int) Math.floor(relativeLon / cellWidth);

		if (latitude == maxLatitude)
			latitudeLocation--;
		if (longitude == maxLongitude)
			longitudeLocation--;
		
		return latitudeLocation + "_" + longitudeLocation;
	}
	
	public boolean insertItem(String id, double latitude, double longitude) {
		if (!validCoordinate(latitude, longitude))
			return false;
		
		String gridIndex = getGridLocation(latitude, longitude);
		return cells.get(gridIndex).addItem(id);
	}
	
	public boolean removeItem(String id, double latitude, double longitude) {
		if (!validCoordinate(latitude, longitude))
			return false;
		
		String gridIndex = getGridLocation(latitude, longitude);
		return cells.get(gridIndex).removeItem(id);
	}
	
	public LinkedList<String> getItems(String id) {
		if (!cells.containsKey(id))
			return new LinkedList<String>();
		return cells.get(id).getAllItems();
	}
}
