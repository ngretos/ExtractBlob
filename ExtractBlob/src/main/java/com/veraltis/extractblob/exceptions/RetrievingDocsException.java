package com.veraltis.extractblob.exceptions;

public class RetrievingDocsException extends BasicException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final String message = "Could not retrieve documents.";

	public RetrievingDocsException() {
		super(message);
	}

	public RetrievingDocsException(String msg) {
		super(msg);
	}

	public RetrievingDocsException(Throwable e) {
		super(e);
	}

	public RetrievingDocsException(String msg, Throwable e) {
		super(msg, e);
	}
}
