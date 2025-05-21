/**
 * 
 */
package com.veraltis.extractblob;

/**
 * 
 */
public class ExtractBlobEclipseMain {
	
	public static void main(String[] args){
		if (args[0].equalsIgnoreCase("/?")) {
			ExtractBlob.help();
			return;
		}

		ExtractBlob extractBlob = new ExtractBlob();
		
		extractBlob.start(args);
	}
}