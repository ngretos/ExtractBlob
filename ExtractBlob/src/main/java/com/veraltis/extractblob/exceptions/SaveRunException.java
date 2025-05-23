package com.veraltis.extractblob.exceptions;

public class SaveRunException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final String message = "Could not save the execution run.";

	public SaveRunException() {
		super(message);
	}

	public SaveRunException(Throwable e) {
		this(message, e);
	}

	public SaveRunException(String msg, Throwable e) {
		super(msg);
		super.initCause(e);
	}
}
