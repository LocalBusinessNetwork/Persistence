package com.rw.persistence;

import com.mongodb.MongoException;

public class RecordNotFoundException extends MongoException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8935399741727963580L;

	public RecordNotFoundException(String msg) {
		super(msg);
		// TODO Auto-generated constructor stub
	}

}
