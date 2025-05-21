package com.veraltis.extractblob;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

class ExtractionRunDetails {
	private ExtractionRun extractionRun;

	private String id;
	private String docId;
	private String relType;
	private String relID;
	private String cId;
	private String accountNo;
	private String actionId;
	
	private List<File> files;

	
	public ExtractionRunDetails(ExtractionRun extractionLog, String docId, String relType, String relID, String cId, String accountNo, String actionId) {
		super();
		this.extractionRun	= extractionLog;
		this.docId			= docId;
		this.relType		= relType;
		this.relID			= relID;
		this.cId			= cId;
		this.accountNo		= accountNo;
		this.actionId		= actionId;
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getDocId() {
		return this.docId;
	}
	public void setDocId(String relId) {
		this.docId = relId;
	}
	public String getRelType() {
		return this.relType;
	}
	public void setRelType(String relType) {
		this.relType = relType;
	}
	public String getRelID() {
		return this.relID;
	}
	public void setRelID(String relID) {
		this.relID = relID;
	}
	public List<File> getFiles() {
		return this.files;
	}
	public void setFiles(List<File> files) {
		this.files = files;
	}
	public boolean addFile(File file) {
		if(this.files == null)
			this.files = new ArrayList<File>();
		
		boolean added = this.files.add(file);
		
		if(added)
			this.extractionRun.setDocumentsExtracted(this.extractionRun.getDocumentsExtracted() + 1);
		
		return added;
	}
	public boolean removeFile(File file) {
		boolean removed = this.files.remove(file);
		
		if(removed)
			this.extractionRun.setDocumentsExtracted(this.extractionRun.getDocumentsExtracted() - 1);
		
		return removed;
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
	public ExtractionRun getExtractionRun() {
		return this.extractionRun;
	}
	public void setExtractionRun(ExtractionRun extractionLog) {
		this.extractionRun = extractionLog;
	}

}