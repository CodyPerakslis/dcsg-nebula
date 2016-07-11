package edu.umn.cs.Nebula.model;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

public class DatabaseConnector {
	// database configuration
	private MysqlDataSource dataSource;
	private Connection dbConnection;

	public DatabaseConnector(String username, String password, String serverName, String databaseName, int port) {
		// database setup
		dataSource = new MysqlDataSource();
		dataSource.setUser(username);
		dataSource.setPassword(password);
		dataSource.setServerName(serverName);
		dataSource.setDatabaseName(databaseName);
		dataSource.setPort(port);
	}

	public boolean connect() {
		try {
			dbConnection = (Connection) dataSource.getConnection();
			return dbConnection != null;
		} catch (SQLException e) {
			System.out.println("[DBCONN] Failed to connect to the database: " + e.getMessage());
		}
		return false;
	}

	public boolean updateQuery(String sqlStatement) {
		try {
			Statement dbStatement = (Statement) dbConnection.createStatement();
			dbStatement.executeUpdate(sqlStatement);
		} catch (SQLException e) {
			System.out.println("[DBCONN] Query <" + sqlStatement + "> failed: " + e);
			return false;
		}
		return true;
	}

	public ResultSet selectQuery(String sqlStatement) {
		ResultSet resultSet;

		try {
			Statement dbStatement = (Statement) dbConnection.createStatement();
			resultSet = dbStatement.executeQuery(sqlStatement);
		} catch (SQLException e) {
			System.out.println("[DBCONN] Query <" + sqlStatement + "> failed: " + e);
			return null;
		}
		return resultSet;
	}

	public boolean isConnected() {
		try {
			return dbConnection.isValid(5);
		} catch (SQLException e) {
			return false;
		}
	}
}
