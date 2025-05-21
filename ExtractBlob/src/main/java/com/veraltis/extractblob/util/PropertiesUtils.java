package com.veraltis.extractblob.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtils {
	
	public static Properties loadProperties(String fileName) throws IOException {
		ClassLoader classLoader = PropertiesUtils.class.getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(fileName);

        Properties config = new Properties();
		config.load(inputStream);
		
		return config;
	}

}
