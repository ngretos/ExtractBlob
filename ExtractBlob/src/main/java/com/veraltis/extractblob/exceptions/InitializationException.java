package com.veraltis.extractblob.exceptions;

public class InitializationException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final String message = "An error occurred during the initialization.";

	public InitializationException() {
		super(message);
	}

	public InitializationException(String msg) {
		super(msg);
	}
	public InitializationException(Throwable e) {
		this(message, e);
	}

	public InitializationException(String msg, Throwable e) {
		super(msg);
		super.initCause(e);
	}
}
