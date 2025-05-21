/**
 * 
 */
package com.veraltis.extractblob;

import org.fusesource.jansi.AnsiConsole;

/**
 * 
 */
public class ExtractBlobJansiMain {
	
	public static void main(String[] args) {
		AnsiConsole.systemInstall();

		if (args[0].equalsIgnoreCase("/?")) {
			ExtractBlob.help();
			return;
		}

		ExtractBlob extractBlob = new ExtractBlob();

		extractBlob.start(args);

		AnsiConsole.systemUninstall();
	}
}
