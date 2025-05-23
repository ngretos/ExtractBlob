package com.veraltis.extractblob.exceptions;

public class AttachementReadingException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final String message = "Could not save attachment.";

	public AttachementReadingException(String msg) {
		super();
	}

	public AttachementReadingException(Throwable e) {
		this(message, e);
	}

	public AttachementReadingException(String msg, Throwable e) {
		super(msg);
		super.initCause(e);
	}
}
