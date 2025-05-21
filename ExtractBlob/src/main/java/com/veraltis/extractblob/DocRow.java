package com.veraltis.extractblob;

import java.sql.Blob;
import java.sql.Timestamp;

class DocRow implements Comparable<DocRow>{
	private String		docId;
	private Timestamp	creationDate;
	private String		relType;
	private String		relId;
	private String		fileName;
	private Blob		docData;
	private String		cId;
	private String		accountNo;
	private String		actionId;
	
	@Override
	public int compareTo(DocRow row) {
		return getCreationDate().compareTo(row.getCreationDate());
	}

	public String getDocId() {
		return this.docId;
	}
	public void setDocId(String relId) {
		this.docId = relId;
	}
	public Timestamp getCreationDate() {
		return this.creationDate;
	}
	public void setCreationDate(Timestamp creationDate) {
		this.creationDate = creationDate;
	}
	public String getRelType() {
		return this.relType;
	}
	public void setRelType(String relType) {
		this.relType = relType;
	}
	public String getRelId() {
		return this.relId;
	}
	public void setRelId(String relID) {
		this.relId = relID;
	}
	public String getFileName() {
		return this.fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public Blob getDocData() {
		return this.docData;
	}
	public void setDocData(Blob docData) {
		this.docData = docData;
	}
	public String getCId() {
		return this.cId;
	}
	public void setCId(String cId) {
		this.cId = cId;
	}
	public String getAccountNo() {
		return this.accountNo;
	}
	public void setAccountNo(String accountNo) {
		this.accountNo = accountNo;
	}
	public String getActionId() {
		return this.actionId;
	}
	public void setActionId(String actionId) {
		this.actionId = actionId;
	}

	@Override
	public String toString() {
		return "Document Name: " + this.fileName + ", Created On: " + this.creationDate;
	}
}