package com.veraltis.extractblob.exceptions;

public class FileSaveException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final String message = "Could not save document.";
	
	public FileSaveException() {
		super(message);
	}

	public FileSaveException(Throwable e) {
		this(message, e);
	}

	public FileSaveException(String msg, Throwable e) {
		super(msg);
		super.initCause(e);
	}
}
