package com.veraltis.extractblob;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

class ExtractionRunDetails {
	
	public static final String	STATUS_SAVED	= "s";
	public static final String	STATUS_FAILED	= "f";

	private ExtractionRun extractionRun;

	private String id;
	private String docId;
	private String relType;
	private String relId;
	private String cId;
	private String accountNo;
	private String actionId;
	private String status = STATUS_SAVED;
	private String failureReason;

	private File file;
	
	private List<File> attachments = new ArrayList<File>();

	
	public ExtractionRunDetails(ExtractionRun extractionLog, String docId, String relType, String relID, String cId, String accountNo, String actionId, File file) {
		super();
		this.extractionRun	= extractionLog;
		this.docId			= docId;
		this.relType		= relType;
		this.relId			= relID;
		this.cId			= cId;
		this.accountNo		= accountNo;
		this.actionId		= actionId;
		this.file			= file;
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
	public String getRelId() {
		return this.relId;
	}
	public void setRelId(String relID) {
		this.relId = relID;
	}
	public List<File> getAttachments() {
		return this.attachments;
	}
	public void setAttachments(List<File> files) {
		this.attachments = files;
	}
	public boolean addAttachment(File file) {
		boolean added = this.attachments.add(file);
		
		if(added)
			this.extractionRun.setDocumentsExtracted(this.extractionRun.getDocumentsExtracted() + 1);
		
		return added;
	}
	public void removeAttachment(File file) {
		if(this.attachments.size() == 0)
			return;
		
		this.attachments.remove(file);
		
		this.extractionRun.setDocumentsExtracted(this.extractionRun.getDocumentsExtracted() - 1);
	}
	public void markAsFailed(String reason) {
		if(this.status.equals(STATUS_FAILED))
			return;

		this.status = STATUS_FAILED;
		this.failureReason = reason;
		this.extractionRun.markAsSucceededWithErrors();
		this.extractionRun.setErroneousRows(this.extractionRun.getErroneousRows() + 1);
		this.extractionRun.setDocumentsExtracted(this.extractionRun.getDocumentsExtracted() - this.attachments.size() - 1);
		this.attachments.removeIf(i -> i != null);
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

	public String getStatus() {
		return this.status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public File getFile() {
		return this.file;
	}

	public void setFile(File file) {
		this.file = file;
	}

	public boolean isSaved() {
		return this.status.equals(STATUS_SAVED);
	}

	public boolean isFailed() {
		return this.status.equals(STATUS_FAILED);
	}

	public String getFailureReason() {
		return this.failureReason;
	}

	public void setFailureReason(String failureReason) {
		this.failureReason = failureReason;
	}
}