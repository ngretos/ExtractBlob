package com.veraltis.extractblob.exceptions;

public class AttachementReadingException extends BasicException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final String message = "Could not save attachment.";

	public AttachementReadingException() {
		super(message);
	}

	public AttachementReadingException(String msg) {
		super(msg);
	}

	public AttachementReadingException(Throwable e) {
		super(e);
	}

	public AttachementReadingException(String msg, Throwable e) {
		super(msg, e);
	}
}
