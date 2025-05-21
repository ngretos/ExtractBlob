package com.veraltis.extractblob.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.dbcp2.BasicDataSource;

@SuppressWarnings("unused")
public class DatabaseAgent {
	
	private static String SQL_SERVER_JDBC_URL_PATTERN =
			"jdbc:sqlserver://{server}:1433;databaseName={databaseName};integratedSecurity=true;encrypt=true;trustServerCertificate=true";
	
	private String userName;
	private String password;
	
	private BasicDataSource ds;

	public DatabaseAgent(String dbServerNameOrIP, String dbDatabaseName) {
		this.ds = new BasicDataSource();
		this.ds.setUrl(getURL(dbServerNameOrIP, dbDatabaseName));
		this.ds.setPoolPreparedStatements(true);
	}

	public Connection getConnection() throws SQLException {
		return this.ds.getConnection();
	}

	public String getURL(String dbServerNameOrIP, String dbDatabaseName) {
		String url = SQL_SERVER_JDBC_URL_PATTERN.replaceFirst("\\{server\\}", dbServerNameOrIP); 
		url = url.replaceFirst("\\{databaseName\\}", dbDatabaseName);
		
		return url;
	}

	public void close() {
		try {
			this.ds.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
