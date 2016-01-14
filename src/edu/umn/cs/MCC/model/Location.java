package edu.umn.cs.MCC.model;

public class Location {
	private int gridSize;
	private int yPosition;
	private int xPosition;

	public Location(int gridSize) {
		this.gridSize = gridSize;
	}

	public Location(int gridSize, double latitude, double longitude) {
		this.gridSize = gridSize;
		setyPosition(latitude);
		setxPosition(longitude);
	}
	
	public Location(int gridSize, int yPosition, int xPosition) {
		this.gridSize = gridSize;
		this.yPosition = yPosition;
		this.xPosition = xPosition;
	}

	public int getyPosition() {
		return yPosition;
	}

	public void setyPosition(double latitude) {
		double latitudeDivider = 181.0 / gridSize;
		this.yPosition = (int) Math.floor((latitude + 90) / latitudeDivider);
	}

	public int getxPosition() {
		return xPosition;
	}

	public void setxPosition(double longitude) {
		double longitudeDivider = 361.0 / gridSize;
		this.xPosition = (int) Math.floor((longitude + 180) / longitudeDivider);
	}


}
