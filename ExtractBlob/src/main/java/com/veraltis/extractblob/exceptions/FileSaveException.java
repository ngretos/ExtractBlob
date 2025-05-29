package com.veraltis.extractblob.exceptions;

public class FileSaveException extends BasicException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final String message = "Could not save document.";
	
	public FileSaveException() {
		super(message);
	}

	public FileSaveException(String msg) {
		super(msg);
	}

	public FileSaveException(Throwable e) {
		super(e);
	}

	public FileSaveException(String msg, Throwable e) {
		super(msg, e);
	}
}
