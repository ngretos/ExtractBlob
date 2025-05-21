package com.veraltis.extractblob.db;

import java.io.IOException;
import java.util.Properties;

import com.veraltis.extractblob.util.PropertiesUtils;

class DBConfig {
	private static final String	SQL_SERVER_JDBC_URL_PATTERN	= "jdbc:sqlserver://{server}:1433;databaseName={databaseName};integratedSecurity=true;encrypt=true;trustServerCertificate=true";
	private static final String	SERVER						= "server";
	private static final String	DATABASE					= "database";

	private Properties configProps;

	DBConfig(String configProperties) throws IOException {
		this.configProps = PropertiesUtils.loadProperties(configProperties);
	}
	
	public String getURL() {
		String url = SQL_SERVER_JDBC_URL_PATTERN.replaceFirst("\\{server\\}", this.configProps.getProperty(SERVER)); 
		url = url.replaceFirst("\\{databaseName\\}", this.configProps.getProperty(DATABASE));
		
		return url;
	}

}