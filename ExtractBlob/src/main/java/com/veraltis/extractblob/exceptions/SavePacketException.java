package com.veraltis.extractblob.exceptions;

public class SavePacketException extends BasicException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final String message = "Could not save packet.";

	public SavePacketException() {
		super(message);
	}

	public SavePacketException(String msg) {
		super(msg);
	}

	public SavePacketException(Throwable e) {
		super(e);
	}

	public SavePacketException(String msg, Throwable e) {
		super(msg, e);
	}
}
