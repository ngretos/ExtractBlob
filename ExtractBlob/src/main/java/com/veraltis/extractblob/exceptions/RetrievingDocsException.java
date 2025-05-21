package com.veraltis.extractblob.exceptions;

public class RetrievingDocsException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final String message = "Could not retrieve documents.";

	public RetrievingDocsException() {
		super(message);
	}

	public RetrievingDocsException(Throwable e) {
		this(message, e);
	}

	public RetrievingDocsException(String msg, Throwable e) {
		super(msg);
		super.initCause(e);
	}
}
