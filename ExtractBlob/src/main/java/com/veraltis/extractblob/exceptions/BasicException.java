package com.veraltis.extractblob.exceptions;

import org.apache.commons.lang3.StringUtils;

public abstract class BasicException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public BasicException() {
		// TODO Auto-generated constructor stub
	}

	public BasicException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	public BasicException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	public BasicException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	public BasicException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}
	
	public String getExtendedMessage() {
		String msg = "";
		
		if(getCause() != null)
			msg += getCause().getMessage();
		
		if (getMessage() != null) {
			if (!msg.equals(StringUtils.EMPTY))
				msg += " -> ";
			msg += getMessage();
		}
		
		return msg;
	}

}
