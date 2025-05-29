package com.veraltis.extractblob.exceptions;

public class SaveRunException extends BasicException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final String message = "Could not save the execution run.";

	public SaveRunException() {
		super(message);
	}

	public SaveRunException(String msg) {
		super(msg);
	}

	public SaveRunException(Throwable e) {
		super(e);
	}

	public SaveRunException(String msg, Throwable e) {
		super(msg, e);
	}
}
