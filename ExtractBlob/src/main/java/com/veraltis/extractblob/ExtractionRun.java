package com.veraltis.extractblob;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class ExtractionRun {

	public static final String	STATUS_SUCCESS				= "s";
	public static final String	STATUS_FAILED				= "f";
	public static final String	STATUS_SUCCESS_WITH_ERRORS	= "ce";

	private int			id;
	private Timestamp	runDate;
	private boolean		initialize;
	private boolean		extractAttachments;
	private Integer		rowsToProcess;
	private Integer		packetSize;
	private Timestamp	processedUpTo;
	private int			erroneousRows;
	private int			rowsProcessed;
	private int			documentsExtracted;
	private String		duration;
	private String		runFor;
	private String		status;

	private List<ExtractionRunDetails> details;
	
	public ExtractionRun(Timestamp	runDate, boolean initialize, boolean extractAttachments, Integer rowsToProcess, Integer packetSize, String runFor) {
		super();
		this.runDate			= runDate;
		this.initialize			= initialize;
		this.extractAttachments	= extractAttachments;
		this.rowsToProcess		= rowsToProcess;
		this.packetSize			= packetSize;
		this.runFor				= runFor;
	}

	ExtractionRunDetails newDetails(String docId, String relType, String relID, String cId, String accountNo, String actionId) {
		ExtractionRunDetails row = new ExtractionRunDetails(this, docId, relType, relID, cId, accountNo, actionId);
		
		this.details.add(row);
		this.rowsProcessed++;
		
		return row;
	}

	public int getId() {
		return this.id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public boolean initialize() {
		return this.initialize;
	}

	public void setInitialize(boolean initialize) {
		this.initialize = initialize;
	}

	public boolean extractAttachments() {
		return this.extractAttachments;
	}

	public void setExtractAttachments(boolean extractAttachments) {
		this.extractAttachments = extractAttachments;
	}

	public Timestamp getProcessedUpTo() {
		return this.processedUpTo;
	}

	public void setProcessedUpTo(Timestamp processedUpTo) {
		this.processedUpTo = processedUpTo;
	}

	public Integer getRowsToProcess() {
		return rowsToProcess;
	}

	public void setRowsToProcess(Integer rowsFetched) {
		this.rowsToProcess = rowsFetched;
	}

	public Integer getPacketSize() {
		return this.packetSize;
	}

	public void setPacketSize(Integer packetSize) {
		this.packetSize = packetSize;
	}

	void setDetails(List<ExtractionRunDetails> rows) {
		this.details = rows;
	}

	List<ExtractionRunDetails> getDetails(){
		return this.details;
	}
	
	public Timestamp getRunDate() {
		return this.runDate;
	}

	public void setRunDate(Timestamp runDate) {
		this.runDate = runDate;
	}

	public String getDuration() {
		return duration;
	}

	public void setDuration(String duration) {
		this.duration = duration;
	}

	void removeDetails() {
		for(ExtractionRunDetails detail : this.details) {
			this.rowsProcessed = this.rowsProcessed - 1;
			this.documentsExtracted = this.documentsExtracted - detail.getAttachments().size() - 1;
		}
		this.details.retainAll(new ArrayList<ExtractionRunDetails>());
	}

	public void newPacket(int packetSize) {
		this.details = new ArrayList<>(packetSize);
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public int getRowsProcessed() {
		return this.rowsProcessed;
	}

	public void setRowsProcessed(int rowsProcessed) {
		this.rowsProcessed = rowsProcessed;
	}

	public int getDocumentsExtracted() {
		return this.documentsExtracted;
	}

	public void setDocumentsExtracted(int documentsExtracted) {
		this.documentsExtracted = documentsExtracted;
	}

	public String getRunFor() {
		return this.runFor;
	}

	public void setRunFor(String runFor) {
		this.runFor = runFor;
	}
	
	public String getExecutionStatusDescr() {
		
		if(this.status == null)
			return null;
		
		if(this.status.equals(STATUS_SUCCESS))
			return "completed successfully";

		if(this.status.equals(STATUS_SUCCESS_WITH_ERRORS))
			return "completed successfully with errors";

		return "failed";
	}

	public int getErroneousRows() {
		return this.erroneousRows;
	}

	public void setErroneousRows(int erroneousRows) {
		this.erroneousRows = erroneousRows;
	}

	public void markAsSucceeded() {
		if(isFailed() || isSucceededWithErrors())
			return;
		this.status = STATUS_SUCCESS;
	}

	public void markAsSucceededWithErrors() {
		if(isFailed())
			return;
		
		this.status = STATUS_SUCCESS_WITH_ERRORS;
	}

	public void markAsFailed() {
		this.status = STATUS_FAILED;
	}
	
	public boolean isSucceeded() {
		return this.status != null && this.status.equals(STATUS_SUCCESS);
	}
	
	public boolean isSucceededWithErrors() {
		return this.status != null && this.status.equals(STATUS_SUCCESS_WITH_ERRORS);
	}
	
	public boolean isFailed() {
		return this.status != null && this.status.equals(STATUS_FAILED);
	}
}
