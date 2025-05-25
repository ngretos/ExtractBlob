/**
 * 
 */
package com.veraltis.extractblob;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.hsmf.MAPIMessage;
import org.apache.poi.hsmf.datatypes.AttachmentChunks;
import org.apache.poi.hsmf.datatypes.ByteChunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.veraltis.extractblob.db.DatabaseAgent;
import com.veraltis.extractblob.exceptions.AttachementReadingException;
import com.veraltis.extractblob.exceptions.FileSaveException;
import com.veraltis.extractblob.exceptions.InitializationException;
import com.veraltis.extractblob.exceptions.RetrievingDocsException;
import com.veraltis.extractblob.exceptions.SavePacketException;
import com.veraltis.extractblob.exceptions.SaveRunException;
import com.veraltis.extractblob.util.PropertiesUtils;

/**
 * 
 */
public class ExtractBlob {

	private final String	PARAM_OUTPUT_DIR				= "d";
	private final String	PARAM_NAME_EXTRACT_ATTACHMENTS	= "a";
	private final String	PARAM_NAME_INITIALIZE			= "i";
	private final String	PARAM_NAME_TIME_TO_RUN			= "rd";
	private final String	PARAM_NAME_ROWS_TO_BE_PROCESSED	= "r";
	private final String	PARAM_NAME_ROWS_PACKET_SIZE		= "p";
	private final String	PARAM_NAME_TEST_MODE			= "t";
	
	private List<String> paramNames = null;

	private final String	UNACCEPTABLE_FILE_NAME_CHARACTERS_REGEX = "[<>:\\/|?*]";

	private final String CONFIG_FILE_NAME = "config.properties";

	private final String	CONFIG_SERVER								= "sqlServerName";
	private final String	CONFIG_DATABASE								= "dbName";
	private final String	CONFIG_LOG_RUN_SERVER						= "logSqlServerName";
	private final String	CONFIG_LOG_RUN_DATABASE						= "logDbName";
	private final String	CONFIG_EXTRACTION_RUN_TABLE					= "extractionRunTableName";
	private final String	CONFIG_EXTRACTION_RUN_DETAILS_TABLE			= "extractionRunDetailsTableName";
	private final String	CONFIG_EXTRACTION_ROOT_DIR					= "extractionRootDir";
	private final String	CONFIG_EXTRACTION_RUN_ROOT_DIR_NAME_PREFIX	= "extractionRunRootDirNamePrefix";
	private final String	CONFIG_ROWS_PACKET_SIZE						= "packetSize";
	private final String	CONFIG_MIME_TYPES_TO_EXCLUDE				= "excludeMimeTypes";
	private final String	CONFIG_MESSAGE_FILE_EXTENSIONS				= "messageFileExtensions";
	private final String	CONFIG_GENERAL_QUERY_CRITERIA				= "generalQueryCriteria";
	private final String	CONFIG_INVOLVED_QUERY_CRITERIA				= "involvedQueryCriteria";
	private final String	CONFIG_ACCOUNTS_QUERY_CRITERIA				= "accountsQueryCriteria";
	private final String	CONFIG_ACTIONS_QUERY_CRITERIA				= "actionsQueryCriteria";
	private final String	CONFIG_GENERAL_QUERY_ORDER_BY				= "generalQueryOrderBy";
	private final String	CONFIG_INVOLVED_QUERY_ORDER_BY				= "involvedQueryOrderBy";
	private final String	CONFIG_ACCOUNTS_QUERY_ORDER_BY				= "accountsQueryOrderBy";
	private final String	CONFIG_ACTIONS_QUERY_ORDER_BY				= "actionsQueryOrderBy";

	Properties config = new Properties();

	private final String	DB_AGENT_SOURCE_NAME	= "SourceDBAgent";
	private final String	DB_AGENT_RUN_NAME		= "RunDBAgent";

	private Hashtable<String, DatabaseAgent> dbAgents;

	private final String SQL_INVOLVED = "select "
			+ "a.crmDOC_ID as DocId, a.crmDOC_CreateDT as DocCreationDate, 'BP' as RelType, c.crmBP_ID as RelId, "
			+ "a.crmDOC_URL as FileName, a.crmDOC_Storage as DocData, d.crmBPR_SR_RefNo as CId, null as AccountNo, null as ActionId "
			+ "from crmDOC a (nolock) join CRMREL b (nolock) on b.crmSOBJ_PR_ID = 'SYS_CRM_DOC' "
			+ "											and b.crmREL_PR_ID = a.crmDOC_ID "
			+ "						join CRMBP c (nolock) on b.crmSOBJ_SR_ID in ('SYS_CRM_IND', 'SYS_CRM_ORG') "
			+ "											and c.crmBP_ID = b.crmREL_SR_ID "
			+ "						join CRMBPR d (nolock) on d.crmBP_SR_ID = c.crmBP_ID "
			+ "											and d.crmBPR_IsValid = 1"
			+ "											and d.crmBPRC_ID ='BPRC_CUSTOMERSHIP' "
			+ "Where crmDOC_StorageType <> 'SYS_DOC_STORAGE_TYPE_URL' "
			+ "  and a.crmdoc_ID in (select {rowsToFetch} m.crmDOC_ID "
			+ "							from crmDOC m (nolock) join CRMREL n (nolock) on n.crmSOBJ_PR_ID = 'SYS_CRM_DOC' "
			+ "																			and n.crmREL_PR_ID = m.crmDOC_ID "
			+ "						where m.crmDOC_StorageType  <> 'SYS_DOC_STORAGE_TYPE_URL' "
			+ "						and n.crmSOBJ_SR_ID in ('SYS_CRM_IND', 'SYS_CRM_ORG', 'SYS_CRM_BE', 'SYS_CRM_BA') "
			+ "						and (m.crmdoc_createdt > cast(? as datetime) or ? is null) "
			+ "						order by m.crmDOC_CreateDT)"
			+ "{additionalFilter} {orderBy}";

	private final String SQL_ACCOUNTS = "select "
			+ "a.crmDOC_ID as DocId, a.crmDOC_CreateDT as DocCreationDate, 'BE' as RelType, d.crmBE_ID as RelId, "
			+ "a.crmDOC_URL as FileName, a.crmDOC_Storage as DocData, d.crwBEX_COFFCustCode as CId, d.crwBEX_COFFRefNo as AccountNo, null as ActionId "
			+ "FROM crmDOC a (nolock) join CRMREL b (nolock) on b.crmSOBJ_PR_ID = 'SYS_CRM_DOC' "
			+ "												and b.crmREL_PR_ID = a.crmDOC_ID "
			+ "						join crwBEX d (nolock) on b.crmSOBJ_SR_ID = 'SYS_CRM_BE' "
			+ "							  				and d.CRMBE_ID = b.crmREL_SR_ID "
			+ "Where crmDOC_StorageType <> 'SYS_DOC_STORAGE_TYPE_URL' "
			+ " and a.crmdoc_ID in (select {rowsToFetch} m.crmDOC_ID "
			+ "							from crmDOC m (nolock) join CRMREL n (nolock) on n.crmSOBJ_PR_ID = 'SYS_CRM_DOC' "
			+ "																			and n.crmREL_PR_ID = m.crmDOC_ID "
			+ "						where m.crmDOC_StorageType  <> 'SYS_DOC_STORAGE_TYPE_URL' "
			+ "						and n.crmSOBJ_SR_ID in ('SYS_CRM_IND', 'SYS_CRM_ORG', 'SYS_CRM_BE', 'SYS_CRM_BA') "
			+ "						and (m.crmdoc_createdt > cast(? as datetime) or ? is null) "
			+ "						order by m.crmDOC_CreateDT)"
			+ "{additionalFilter} {orderBy}";

	private final String SQL_ACTIONS = "select "
			+ "a.crmDOC_ID as DocId, a.crmDOC_CreateDT as DocCreationDate, 'BA' as RelType, c.crmBA_ID as RelId, "
			+ "a.crmDOC_URL as FileName, a.crmDOC_Storage as DocData, d.crwBEX_COFFCustCode as CId, e.crmCOFF_RefNo as AccountNo, c.crmBA_Id as ActionId "
			+ "FROM crmDOC a (nolock) join CRMREL b (nolock) on b.crmSOBJ_PR_ID = 'SYS_CRM_DOC' "
			+ "												and b.crmREL_PR_ID = a.crmDOC_ID "
			+ "							join CRMBA c (nolock) on b.crmSOBJ_SR_ID = 'SYS_CRM_BA' "
			+ "												and b.crmREL_SR_ID = c.CRMBA_ID "
			+ "							join crwBEX d (nolock) on d.CRMBE_ID = c.CRMBE_ID "
			+ "							join crmcoff e (nolock) on e.crmCOFF_RefNo = d.crwBEX_COFFRefNo "
			+ "Where crmDOC_StorageType <> 'SYS_DOC_STORAGE_TYPE_URL' "
			+ "  and a.crmdoc_ID in (select {rowsToFetch} m.crmDOC_ID "
			+ "							from crmDOC m (nolock) join CRMREL n (nolock) on n.crmSOBJ_PR_ID = 'SYS_CRM_DOC' "
			+ "																			and n.crmREL_PR_ID = m.crmDOC_ID "
			+ "						where m.crmDOC_StorageType  <> 'SYS_DOC_STORAGE_TYPE_URL' "
			+ "						and n.crmSOBJ_SR_ID in ('SYS_CRM_IND', 'SYS_CRM_ORG', 'SYS_CRM_BE', 'SYS_CRM_BA') "
			+ "						and (m.crmdoc_createdt > cast(? as datetime) or ? is null) "
			+ "						order by m.crmDOC_CreateDT)"
			+ "{additionalFilter} {orderBy}";

	private final String SQL_CREATE_RUN_TABLE = "IF OBJECT_ID (N'dbo.{tableName}', N'U') IS NULL "
			+ "	CREATE TABLE dbo.{tableName}( "
			+ "		Id int identity(1, 1), "
			+ "		RunDate datetime, "
			+ "		Initialize bit, "
			+ "		ExtractAttachments bit, "
			+ "		RowsToProcess int, "
			+ "		PacketSize int, "
			+ "		RunFor varchar(15), "
			+ "		RowsProcessed int, "
			+ "		DocumentsExtracted int, "
			+ "		ProcessedUpTo datetime, "
			+ "		Duration varchar(15), "
			+ "		Status varchar(2), "
			+ "		CONSTRAINT {tableName}_PK PRIMARY KEY (Id) "
			+ "	) ON [PRIMARY];";

	private final String SQL_CREATE_RUN_DETAILS_TABLE = "IF OBJECT_ID (N'dbo.{tableName}', N'U') IS NULL "
			+ "	CREATE TABLE dbo.{tableName}( "
			+ "		Id int identity(1, 1), "
			+ "		{refTableName}_Id int , "
			+ "		Path nvarchar(max), "
			+ "		DocId nvarchar(max), "
			+ "		RelType nvarchar(max), "
			+ "		RelID nvarchar(max), "
			+ "		CId nvarchar(max) NULL, "
			+ "		AccountNo nvarchar(max) NULL, "
			+ "		ActionId nvarchar(max) NULL, "
			+ "		Status varchar(2), "
			+ "		CONSTRAINT {tableName}_PK PRIMARY KEY (Id), "
			+ "		CONSTRAINT {tableName}_{refTableName}_FK FOREIGN KEY ({refTableName}_Id) REFERENCES dbo.{refTableName}(Id) ON DELETE CASCADE"
			+ "	) ON [PRIMARY];";

	private final String	SQL_TABLE_NAME_SUBST_VARIABLE				= "\\{tableName\\}";
	private final String	SQL_REF_TABLE_NAME_SUBST_VARIABLE			= "\\{refTableName\\}";
	private final String	SQL_ROWS_TO_FETCH_SUBST_VARIABLE			= "\\{rowsToFetch\\}";
	private final String	SQL_ADDITIONAL_FILTER_NAME_SUBST_VARIABLE	= "\\{additionalFilter\\}";
	private final String	SQL_ORDER_BY_NAME_SUBST_VARIABLE			= "\\{orderBy\\}";

	private final String SQL_DROP_TABLE = "IF OBJECT_ID (N'dbo.{tableName}', N'U') IS NOT NULL drop table {tableName};";

	private final String	SQL_SAVE_INITIAL_RUN				= "insert {tableName}(RunDate, Initialize, ExtractAttachments, RowsToProcess, PacketSize, RunFor) values(?, ?, ?, ?, ?, ?)";
	private final String	SQL_SAVE_RUN						= "Update {tableName} set RowsProcessed = ?, DocumentsExtracted = ?, ProcessedUpTo = ?, Status = ?, Duration = ?  Where Id = ?";
	private final String	SQL_SAVE_RUN_DETAILS				= "insert {tableName}({refTableName}_Id, DocId, RelType, RelId, Cid, AccountNo, ActionId, Path, Status, Reason) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	private final String	SQL_DELETE_RUN 						= "Delete {tableName} Where Id = ?";

	private final String SQL_MAX_PROCESSED_DATE = "select max(ProcessedUpTo) from {tableName}";

	private Hashtable<String, String> params;
	
	private List<String> excludeMimeTypes;
	private List<String> messageFileExtensions;
	
	private ExtractionRun extractionRun;
	private String extractionFolder;

	private Logger	logger			= LoggerFactory.getLogger("General");
	private Logger	newLineLogger	= LoggerFactory.getLogger("NewLine");
	private Logger	docloggerStart	= LoggerFactory.getLogger("ConsoleLoggerStart");
	private Logger	docloggerDocNo	= LoggerFactory.getLogger("ConsoleLoggerDocNo");
	private Logger	docloggerEnd	= LoggerFactory.getLogger("ConsoleLoggerEnd");

	private LocalDateTime startTime = LocalDateTime.now();
	private LocalDateTime endTime;
	
	private Boolean shouldInitialize = null;
	private boolean stopExecution = false;
	private boolean runPeriodExpired = false;
	
	/**
	 * -d:  The root folder where the files will be extracted.
	 * -a:  Extracts the attachments form mail messages recursively. Defaults to false.
	 * -i:  Initialization. Cleans the output folder and truncates the log. 
	 * 		It will be ignored in test mode. Defaults to false.
	 * -rd: How long (in hours) the program will be running. If it has been specified together with -r 
	 * 		the program will stop when ever any of this conditions comes first. Defaults to 5h.
	 * -r:  Rows to be processed (for test purposes only). Defaults to all.
	 * -p:  The size of rows segment fetched from the database.
			If it omitted packet size retrieve is't value from the configuration file.
			If neither the parameter nor the configuration value is defined an exception is thrown.
	 * -t:  Run in test mode. No files extracted nor ExtractionRun rows produced.
	 */
	public static void help() {
		System.out.println("Arotron Documents Extraction Utility.");
		System.out.println("\n");
		System.out.println("Parameters List");
		System.out.println("-d:\tThe root folder where the files will be extracted.");
		System.out.println("-a:\tExtracts the attachments form mail messages recursively. Defaults to false.");
		System.out.println("-i:\tInitialization. Cleans the output folder and truncates the log. Defaults to false.");
		System.out.println("\tIt will be ignored in test mode. Defaults to false. Defaults to false.");
		System.out.println("-rd:\tHow long the program will be running. If it has been specified together with -r");
		System.out.println("\tthe program will stop when ever any of this conditions comes first. Defaults to 5h.");
		System.out.println("-r:\tRows to be processed. Defaults to all.");
		System.out.println("-p:\tThe size of rows segment fetched from the database.");
		System.out.println("\tIf it omitted packet size retrieve is't value from the configuration file (currently 500.");
		System.out.println("\tIf neither the parameter nor the configuration value is defined an exception is thrown.");
		System.out.println("-t:\tRun in test mode. No files extracted nor ExtractionRun rows produced.");
	}

	private Hashtable<String, String> parseParams(String[] args) throws InitializationException {
		Hashtable<String, String> params = new Hashtable<String, String>(2);

		for(String arg : args) {
			
			String[] keyAndValue = getKeyAndValue(arg);

			if(keyAndValue[0].equalsIgnoreCase(this.PARAM_NAME_EXTRACT_ATTACHMENTS) ||
					keyAndValue[0].equalsIgnoreCase(this.PARAM_NAME_INITIALIZE) ||
					keyAndValue[0].equalsIgnoreCase(this.PARAM_NAME_TEST_MODE)) {
				if(StringUtils.trimToEmpty(keyAndValue[1]).equals(StringUtils.EMPTY))
					params.put(keyAndValue[0], "true");
				else 
					params.put(keyAndValue[0], keyAndValue[1]);
			}
			else
				if(!StringUtils.trimToEmpty(keyAndValue[1]).equals(StringUtils.EMPTY))
					params.put(keyAndValue[0], keyAndValue[1]);
				else
					throw new InitializationException("Parameter's [" + keyAndValue[0] + "] value is not valid.");
		}
			
		try {
			int i = Integer.parseInt(params.get(PARAM_NAME_ROWS_PACKET_SIZE));
			if(!(i > 0))
				throw new InitializationException("Invalid parameter [" + PARAM_NAME_ROWS_PACKET_SIZE + "].");
		}
		catch(NumberFormatException e) {
			throw new InitializationException("Invalid parameter [" + PARAM_NAME_ROWS_PACKET_SIZE + "].");
		}
		
		if(params.get(PARAM_NAME_TIME_TO_RUN) == null)
			params.put(PARAM_NAME_TIME_TO_RUN, "5h");
		
		return params;
	}

	private String[] getKeyAndValue(String arg) throws InitializationException {
		if(!arg.startsWith("-")) 
			throw new InitializationException("Invalid parameter [" + arg + "]. Use [-] before parameter names.");

		arg = arg.replaceAll("-", StringUtils.EMPTY);
		
		List<String> paramNames = getParamNames(); 

		String[] keyAndValue = new String[2];
		
		for(String paramName : paramNames) {
			if(arg.toLowerCase().startsWith(paramName)) {
				keyAndValue[0] = paramName;
				keyAndValue[1] = arg.replaceFirst(paramName, StringUtils.EMPTY);
				return keyAndValue;
			}
		}

		throw new InitializationException("Parameter [" + arg + "] is not recognized.");
	}

	private List<String> getParamNames() {
		if (this.paramNames == null) {
			this.paramNames = Arrays.asList(this.PARAM_OUTPUT_DIR, this.PARAM_NAME_EXTRACT_ATTACHMENTS,
					this.PARAM_NAME_INITIALIZE, this.PARAM_NAME_ROWS_TO_BE_PROCESSED, this.PARAM_NAME_ROWS_PACKET_SIZE,
					this.PARAM_NAME_TIME_TO_RUN, this.PARAM_NAME_TEST_MODE);

			Collections.sort(this.paramNames, Comparator.comparing(String :: length).reversed());
		}

		return this.paramNames;
	}

	public void start(String[] args) {

		try {
			initialize(parseParams(args));

			logNewLine();
			info("Task started" + (isTestMode() ? " (Test Mode: No actual files will be extracted)" : StringUtils.EMPTY) + ".");
			
			extract();
		}
		catch (InitializationException e) {
			error(e.getMessage(), e);
		}
		finally {
			closeDBAgents();
			logNewLine();
			String msg = "Task ";
			
			if(this.extractionRun != null)
				msg += this.extractionRun.getExecutionStatusDescr() 
				+ ". Rows processed: " + this.extractionRun.getRowsProcessed()
				+ " (Erroneous rows: " + this.extractionRun.getErroneousRows()
				+ "). Documents extracted: " + this.extractionRun.getDocumentsExtracted()
				+ ". Duration: " + this.extractionRun.getDuration()
				+ (isTestMode() ? " (Test Mode: No actual files extracted.)" : StringUtils.SPACE) + ".";
			else
				msg += "failed.";

			if(msg.contains("failed"))
				error(msg);
			else if(msg.contains("successfully") && msg.contains("error"))
				warn(msg);
			else 
				info(msg); 
		}
	}

	private void closeDBAgents() {
		if(this.dbAgents == null)
			return;
		
		for(String name : this.dbAgents.keySet()) {
			DatabaseAgent dbAgent = this.dbAgents.get(name); 
			if(dbAgent != null)
				this.dbAgents.get(name).close();
		}
	}

	private String getTaskDuration(LocalDateTime startAt, LocalDateTime endtAt) {
		String duration = StringUtils.EMPTY;
		
		long hours = 0, minutes = 0, seconds = 0;
		
		hours = ChronoUnit.HOURS.between(startAt, endtAt);
		minutes = ChronoUnit.MINUTES.between(startAt.plusHours(hours), endtAt);
		seconds = ChronoUnit.SECONDS.between(startAt.plusHours(hours).plusMinutes(minutes), endtAt);
		
		if(hours > 0)
			duration += String.valueOf(hours) + "h";
		
		if(minutes > 0) {
			if(!duration.isEmpty())
				duration += StringUtils.SPACE;
			duration += String.valueOf(minutes) + "m";
		}
		
		if(seconds > 0 || !duration.isEmpty()) {
			if(!duration.isEmpty())
				duration += StringUtils.SPACE;
			duration += String.valueOf(seconds) + "s";
			
		}

		return duration;
	}

	private void initialize(Hashtable<String, String> params) throws InitializationException {
		info("Initialize Tool...");
		
		this.params = params;
		
		try {
			loadConfiguration();

			if(StringUtils.trimToEmpty(params.get(this.PARAM_NAME_ROWS_PACKET_SIZE)).equals(StringUtils.EMPTY) &&
					StringUtils.trimToEmpty(getConfig(this.CONFIG_ROWS_PACKET_SIZE)).equals(StringUtils.EMPTY))
				throw new IllegalArgumentException("Neither parameter [Packet Size] nor the corresponding configuration has been specified.");

			initializeExtractionRootDir();
			initializeDBAgents();
			initializeRunTables();

			createExtractionRun();

			this.extractionFolder = getParam(this.PARAM_OUTPUT_DIR) + "/"
					+ getConfig(this.CONFIG_EXTRACTION_ROOT_DIR) + "/"
					+ (!StringUtils.trimToEmpty(getConfig(this.CONFIG_EXTRACTION_RUN_ROOT_DIR_NAME_PREFIX)).equals(StringUtils.EMPTY) ?
							getConfig(this.CONFIG_EXTRACTION_RUN_ROOT_DIR_NAME_PREFIX):
							"Run_")
					+ replaceUnacceptableCharacters(this.extractionRun.getRunDate().toString());

			excludeMimeTypes = Arrays.asList(StringUtils.split(getConfig(CONFIG_MIME_TYPES_TO_EXCLUDE), ','));
			messageFileExtensions = Arrays.asList(StringUtils.split(getConfig(CONFIG_MESSAGE_FILE_EXTENSIONS), ','));
			
			this.extractionRun.setRunFor(params.get(PARAM_NAME_TIME_TO_RUN));
			
			this.endTime = calculateEndTime();
		}
		catch (IOException | SQLException | DateTimeException | ArithmeticException e) {
			throw new InitializationException(e);
		}
	}

	private String replaceUnacceptableCharacters(String filename) {
		return replaceUnacceptableCharacters(filename, "_");
	}

	private String replaceUnacceptableCharacters(String fileName, String replacement) {
		return fileName.replaceAll(UNACCEPTABLE_FILE_NAME_CHARACTERS_REGEX, replacement);
	}

	private LocalDateTime calculateEndTime() {
		Duration d = Duration.parse("pt" + this.extractionRun.getRunFor());
		return this.extractionRun.getRunDate().toLocalDateTime().plus(d);
	}

	private void loadConfiguration() throws FileNotFoundException, IOException {
        this.config = PropertiesUtils.loadProperties(CONFIG_FILE_NAME);
	}

	private void extract() {
		int packetSize = getParam(this.PARAM_NAME_ROWS_PACKET_SIZE) != null ? 
				Integer.parseInt(getParam(this.PARAM_NAME_ROWS_PACKET_SIZE)) :
					Integer.parseInt(getConfig(CONFIG_ROWS_PACKET_SIZE));
		
		String[] packetSizes = null;
		
		if(getParam(PARAM_NAME_ROWS_TO_BE_PROCESSED) != null) {
			int rowsToFetch = Integer.parseInt(getParam(PARAM_NAME_ROWS_TO_BE_PROCESSED));

			if(rowsToFetch % packetSize == 0)
				packetSizes = new String[rowsToFetch / packetSize];
			else
				packetSizes = new String[rowsToFetch / packetSize + 1];

			for(int i = 0; i < packetSizes.length; i++) {
				packetSizes[i] = String.valueOf(Math.min(packetSize, rowsToFetch - i*packetSize));
			}
		}
			
		List<DocRow> docRows;

		try {
			for(int i = 0; packetSizes == null || i < packetSizes.length; i++) {

				if(stopExecution() || runPeriodExpired())
					break;

				String actualPacketSize = packetSizes != null ? packetSizes[i] : String.valueOf(packetSize); 
				
				logNewLine();

				String msg = "Packet " + (i + 1);
				
				if(packetSizes != null)
					msg += " out of " + packetSizes.length;
				
				msg +=	". Packet size: " + actualPacketSize + " (Rows "  + (i*packetSize + 1) + " to " + (i*packetSize + Integer.parseInt(actualPacketSize)) + ").";

				info(msg);
				info("Retrieving documents from database... ");

				this.extractionRun.newPacket(Integer.parseInt(actualPacketSize));
				
				docRows = getDocsFromDB(constructSQLForInvolved(actualPacketSize));
				docRows.addAll(getDocsFromDB(constructSQLForAccounts(actualPacketSize)));
				docRows.addAll(getDocsFromDB(constructSQLForActions(actualPacketSize)));

				if(docRows.isEmpty()) {
					info(msg + " No rows found.");
					break;
				}
				else {
					msg += " Rows fetched: " + docRows.size();
					extract(docRows, msg);
					finalizePacketProcessing();
				}
			}
			this.extractionRun.markAsSucceeded();
		}
		catch (RetrievingDocsException e) {
			this.extractionRun.markAsSucceededWithErrors();
			error("Error while retrieving rows for packet.", e);
		}
		finally {
			info("Finalizing Run...");
			this.extractionRun.setDuration(getTaskDuration(this.extractionRun.getRunDate().toLocalDateTime(), LocalDateTime.now()));
			try {
				saveRun();
			}
			catch (SaveRunException e) {
				this.extractionRun.markAsFailed();
				this.extractionRun.setRowsProcessed(0);
				this.extractionRun.setDocumentsExtracted(0);
				error(e.getMessage() + " Rolling back...", e);
				deleteFolder(Paths.get(this.extractionFolder));
				deleteRunFromDatabase();
			}
		}
	}

	private void handleSavePacketException(SavePacketException e) {
		error(e.getMessage(), e.getCause());
		deletePacketFiles(e.getMessage());
	}

	private void finalizePacketProcessing() {
		info("Finalizing packet processing...");
		try {
			savePacket();
		}
		catch (SavePacketException e) {
			handleSavePacketException(e);
		}
	}

	private void createExtractionRun() throws InitializationException {
		Integer rowsToBeProcessed = getParam(this.PARAM_NAME_ROWS_TO_BE_PROCESSED) != null
				? Integer.valueOf(getParam(this.PARAM_NAME_ROWS_TO_BE_PROCESSED))
				: null;

		int packetSize = getParam(this.PARAM_NAME_ROWS_PACKET_SIZE) != null
				? Integer.valueOf(getParam(this.PARAM_NAME_ROWS_PACKET_SIZE))
				: Integer.valueOf(getConfig(this.CONFIG_ROWS_PACKET_SIZE));

		this.extractionRun =
				new ExtractionRun(Timestamp.valueOf(this.startTime), shouldInitialize(),
						extractAttachemts(), rowsToBeProcessed, packetSize, 
						getParam(PARAM_NAME_TIME_TO_RUN));

		try {
			saveInitialRunRow();
			this.extractionRun.setProcessedUpTo(getLastProcessedDate());
		}
		catch (SQLException e) {
			throw new InitializationException(e);
		}
	}

	private String constructSQLForInvolved(String packetSize) {
		return commonSQL(SQL_INVOLVED, 
				StringUtils.stripToEmpty(getConfig(CONFIG_INVOLVED_QUERY_CRITERIA)), 
				StringUtils.stripToEmpty(getConfig(CONFIG_INVOLVED_QUERY_ORDER_BY)),
				packetSize);
	}

	private String constructSQLForAccounts(String packetSize) {
		return commonSQL(SQL_ACCOUNTS, 
				StringUtils.stripToEmpty(getConfig(CONFIG_ACCOUNTS_QUERY_CRITERIA)), 
				StringUtils.stripToEmpty(getConfig(CONFIG_ACCOUNTS_QUERY_ORDER_BY)),
				packetSize);
	}

	private String constructSQLForActions(String packetSize) {
		return commonSQL(SQL_ACTIONS, 
				StringUtils.stripToEmpty(getConfig(CONFIG_ACTIONS_QUERY_CRITERIA)), 
				StringUtils.stripToEmpty(getConfig(CONFIG_ACTIONS_QUERY_ORDER_BY)),
				packetSize);
	}

	private String commonSQL(String sql, String filter, String orderBy, String packetSize) {
		sql = sql.replaceAll(this.SQL_ROWS_TO_FETCH_SUBST_VARIABLE, "top (" + packetSize + ")");

		sql = sql.replaceAll(this.SQL_ADDITIONAL_FILTER_NAME_SUBST_VARIABLE, 
				StringUtils.SPACE + StringUtils.stripToEmpty(getConfig(CONFIG_GENERAL_QUERY_CRITERIA))
				+ StringUtils.SPACE + StringUtils.stripToEmpty(filter));

		if(!(StringUtils.stripToEmpty(getConfig(CONFIG_GENERAL_QUERY_ORDER_BY)).equals(StringUtils.EMPTY) && 
				orderBy.equals(StringUtils.EMPTY))) {
			orderBy = "order by"
					+ StringUtils.SPACE + StringUtils.stripToEmpty(getConfig(CONFIG_GENERAL_QUERY_ORDER_BY))
					+ StringUtils.SPACE + StringUtils.stripToEmpty(orderBy);
		}

		sql = sql.replaceAll(this.SQL_ORDER_BY_NAME_SUBST_VARIABLE, orderBy);

		return sql;
	}

	private void extract(List<DocRow> rows, String logMsgPrefix){
		Collections.sort(rows, Comparator.comparing(DocRow :: getCreationDate));

		info("Extracting...");
		logNewLine();

		String targetFolder, filename;
		int i = 0;

		for(DocRow row: rows) {
			
			if(stopExecution() || runPeriodExpired())
				break;

			i++;

			targetFolder = constructTargetFolderPath(row);
			filename = constructFileName(row);
			
			String logFileMsg = logMsgPrefix + ". ";
			ExtractionRunDetails runDetails = null;
			File file = null;
			
			try {
				try {
					if(isMessageFile(row.getFileName()) && extractAttachemts()) {
						targetFolder = getDistinctivePath(targetFolder, constructFileName(row));
						filename = row.getFileName();
					}
				}
				finally {
					file = new File(targetFolder, filename);
					runDetails = 
							getRunDetails(row.getDocId(), row.getRelType(), row.getRelId(), row.getCId(), row.getAccountNo(), row.getActionId(), file);
				}

				save(row.getDocData(), file);

				if(isMessageFile(row.getFileName()) && extractAttachemts()) {
					try {
						extractAndSaveAttachments(row.getDocData(), targetFolder, runDetails, 1);
					}
					catch (AttachementReadingException | FileSaveException e) {
						deleteFolder(file.getParentFile().toPath());
						throw e;
					}
				}

				logFileMsg += "Extracted "; 
			}
			catch (AttachementReadingException | FileSaveException  e) {
				logFileMsg += "Failed to extract ";
				runDetails.markAsFailed(e.getMessage());
				deleteFile(file, true);
			}
			finally {
				this.extractionRun.setProcessedUpTo(row.getCreationDate());
				logFile(logFileMsg + "document", i, row.getFileName() + 
						 " to " + Paths.get(this.extractionFolder).relativize(Paths.get(file.getPath())),
						 !runDetails.isSaved());
			}
		}
	}

	private void extractAndSaveAttachments(Blob docData, String targetFolder, ExtractionRunDetails runDetails, int identation) throws AttachementReadingException, FileSaveException {
		try(MAPIMessage msg = new MAPIMessage(docData.getBinaryStream())){
			extractAndSaveAttachments(msg, targetFolder, runDetails, 1);
		}
		catch (SQLException e) {
			throw new AttachementReadingException("Could not access blob data (Doc_Id: " + runDetails.getDocId() + ".", e);
		}
		catch (IOException e) {
			throw new AttachementReadingException("Could not read blob data (Doc_Id: " + runDetails.getDocId() + ".", e);
		}
	}
	
	private void extractAndSaveAttachments(MAPIMessage msg, String targetFolder, ExtractionRunDetails runDetails, int identation) throws FileSaveException, AttachementReadingException{
		String identationStr = "\t".repeat(identation);
		int  attachemtNo = 0;
		
		AttachmentChunks[] attachedFiles = msg.getAttachmentFiles();
		
		for(AttachmentChunks chunks : attachedFiles) {
					
			if(chunks.getAttachMimeTag() != null && excludeMimeTypes.contains(chunks.getAttachMimeTag().getValue()) && 
					chunks.getAttachFileName().getValue().startsWith("image00"))
				continue;
			
			if(chunks.getAttachData() == null)
				continue;

			String filename = chunks.getAttachDisplayName() != null ? chunks.getAttachDisplayName().toString() : chunks.getAttachFileName().toString();
			
			if(filename == null)
				filename = "attachment";
			
			File file = new File(targetFolder, getDistinctiveFileName(targetFolder, filename));
			try {
				save(chunks.getAttachData(), file);
				runDetails.addAttachment(file);

				logFile(identationStr + "Extracted attachment", ++attachemtNo, filename
						+ " to " + Paths.get(this.extractionFolder).relativize(Paths.get(file.getPath())), !runDetails.isSaved());
			}
			catch (FileSaveException e) {
				throw new FileSaveException("Could not save attachment (Doc_Id: " + runDetails.getDocId()
						+ ", Attachment name: " + filename + ").", e);
			}

			if(isMessageFile(chunks)) {
				try(MAPIMessage attachedMsg = new MAPIMessage(new ByteArrayInputStream(chunks.getAttachData().getValue()))){
					extractAndSaveAttachments(attachedMsg, getDistinctivePath(targetFolder, chunks.getAttachDisplayName().toString()), runDetails, identation + 1);
				}
				catch (Exception e) {
					throw new AttachementReadingException(
							"Could not read attached mail data (Doc_Id: " + runDetails.getDocId()
									+ ", Attachment name: " + chunks.getAttachDisplayName().toString() + ").", e);
				}
			}
		}
	}

	private String constructTargetFolderPath(DocRow row) {
		String rootFolder = this.extractionFolder;
		
		if(row.getCId() != null)
			rootFolder += "/CID/" + row.getCId();
		if(row.getAccountNo() != null)
			rootFolder += "/Account/" + row.getAccountNo();
		if(row.getActionId() != null)
			rootFolder += "/Action/" + row.getActionId();
		
		return rootFolder;
	}

	private String constructFileName(DocRow row) {
		return row.getDocId() + "_" + row.getFileName();
	}

	private String getDistinctivePath(String folder, String fileName) throws FileSaveException {
		return folder + "/" + fileName + getDistinctiveNumber(folder, fileName);
	}

	private String getDistinctiveFileName(String folder, String fileName) throws FileSaveException{
		String distinctiveNumberStr = getDistinctiveNumber(folder, fileName);
		
		if(distinctiveNumberStr.equals(StringUtils.EMPTY))
			return fileName;
		
		String[] nameParts = fileName.split("/.");

		String distinctiveName = 
				fileName.replaceAll("." + nameParts[nameParts.length - 1], distinctiveNumberStr + "." + nameParts[nameParts.length - 1]);

		if(nameParts.length > 1)
			distinctiveName =
			fileName.replaceAll("." + nameParts[nameParts.length - 1], distinctiveNumberStr + "." + nameParts[nameParts.length - 1]);
		else
			distinctiveName += fileName + StringUtils.SPACE + distinctiveNumberStr;
			
		return distinctiveName;
	}

	private String getDistinctiveNumber(String folder, String fileName) throws FileSaveException{
		int number = 0;
		Path path = Paths.get(folder);
		
		if(Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
			try (Stream<Path> stream = Files.list(Paths.get(folder))) {
			    
				number = stream
				      .filter(file -> file.getFileName().toString().startsWith(fileName))
				      .collect(Collectors.toSet()).size();
				}
			catch(IOException e) {
				throw new FileSaveException(e);
			}
		}
		
		return number > 0 ? StringUtils.SPACE + "(" + String.valueOf(number + 1) + ")": StringUtils.EMPTY;
	}

	private void save(ByteChunk attachedData, File file) throws FileSaveException {
		try(ByteArrayInputStream is = new ByteArrayInputStream(attachedData.getValue())){
			save(is, file);
		}
		catch (Exception e) {
			throw new FileSaveException(e);
		}
	}

	private void save(Blob docData, File file) throws FileSaveException {
		try {
			save(docData.getBinaryStream(), file);
		}
		catch (Exception e) {
			throw new FileSaveException(e);
		}
	}

	private void save(InputStream inputStream, File file) throws Exception {
		if(isTestMode())
			return;

		File folder = file.getParentFile();
			
		folder.mkdirs();

		try(OutputStream outputStream = new FileOutputStream(file)){
			
	        int bytesRead = -1;
	        byte[] buffer = new byte[1024];
	        
	        while ((bytesRead = inputStream.read(buffer)) != -1) {
	            outputStream.write(buffer, 0, bytesRead);
	        }
		}
	}

	private boolean isMessageFile(String fileName)
	{
		for(String extension : messageFileExtensions) {
			if(fileName.endsWith(extension))
				return true;
		}
		return false;
	}
	
	private boolean isMessageFile(AttachmentChunks chunks)
	{
		return chunks.getAttachExtension() != null && 
				this.messageFileExtensions.contains(chunks.getAttachExtension().getValue());
	}
	
	private String getConfig(String propertyName) {
		return this.config.getProperty(propertyName);
	}

	private String getParam(String paramName) {
		return this.params.get(paramName);
	}

	private List<DocRow> getDocsFromDB(String sql) throws RetrievingDocsException {
		try (Connection conn = getDBAgent(this.DB_AGENT_SOURCE_NAME).getConnection(); 
				PreparedStatement stmnt = conn.prepareStatement(sql);){

			int i = 1;
			if(this.extractionRun.getProcessedUpTo() != null) {
				stmnt.setTimestamp(i++, this.extractionRun.getProcessedUpTo());
				stmnt.setTimestamp(i++, this.extractionRun.getProcessedUpTo());
			}
			else {
				stmnt.setNull(i++, Types.TIMESTAMP);
				stmnt.setNull(i++, Types.TIMESTAMP);
			}

			ResultSet resultSet = stmnt.executeQuery();
			List<DocRow> rows = new ArrayList<DocRow>();
			
			while(resultSet.next()) {
				DocRow docRow = new DocRow();

				docRow.setDocId(resultSet.getString("DocId"));
				docRow.setCreationDate(resultSet.getTimestamp("DocCreationDate"));
				docRow.setRelId(resultSet.getString("RelId"));
				docRow.setRelType(resultSet.getString("RelType"));
				docRow.setFileName(resultSet.getString("FileName"));
				docRow.setDocData(resultSet.getBlob("DocData"));
				docRow.setCId(resultSet.getString("CId"));
				docRow.setAccountNo(resultSet.getString("AccountNo"));
				docRow.setActionId(resultSet.getString("ActionId"));
				
				rows.add(docRow);
			}
			
			return rows;
		}
		catch(SQLException e) {
			throw new RetrievingDocsException("Could not retrieve docs from database.", e);
		}
	}

	private DatabaseAgent getDBAgent(String databaseAgentName) {
		return this.dbAgents.get(databaseAgentName);
	}

	private ExtractionRunDetails getRunDetails(String docId, String relType, String relID, String cId, String accountNo,
			String actionId, File file) {
		return this.extractionRun.newDetails(docId, relType, relID, cId, accountNo, actionId, file);
	}
	
	private void saveInitialRunRow() throws SQLException {
		if(isTestMode())
			return;
		
		String sql = this.SQL_SAVE_INITIAL_RUN.replaceAll(this.SQL_TABLE_NAME_SUBST_VARIABLE, getConfig(CONFIG_EXTRACTION_RUN_TABLE));

		executeSaveInitialRunSQL(sql);
	}

	private void executeSaveInitialRunSQL(String sql) throws SQLException {
		try (Connection conn = getDBAgent(this.DB_AGENT_RUN_NAME).getConnection();
				PreparedStatement stmnt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);) {
			
			int i = 1;
			stmnt.setTimestamp(i++, this.extractionRun.getRunDate());
			stmnt.setBoolean(i++, this.extractionRun.initialize());
			stmnt.setBoolean(i++, this.extractionRun.extractAttachments());
			if(this.extractionRun.getRowsToProcess() != null)
				stmnt.setInt(i++, this.extractionRun.getRowsToProcess());
			else
				stmnt.setNull(i++, Types.INTEGER);
			stmnt.setInt(i++, this.extractionRun.getPacketSize());
			stmnt.setString(i++, this.extractionRun.getRunFor());

			stmnt.executeUpdate();
			
			ResultSet rs = stmnt.getGeneratedKeys();
			rs.next();
			
			this.extractionRun.setId(rs.getInt(1));
		}
	}

	private void savePacket() throws SavePacketException  {
		info("Saving packet to database...");
		
		if(isTestMode())
			return;

		String sql = this.SQL_SAVE_RUN_DETAILS.replaceAll(this.SQL_TABLE_NAME_SUBST_VARIABLE, getConfig(CONFIG_EXTRACTION_RUN_DETAILS_TABLE))
				.replaceAll(this.SQL_REF_TABLE_NAME_SUBST_VARIABLE, getConfig(CONFIG_EXTRACTION_RUN_TABLE));
		
		try (Connection conn = getDBAgent(this.DB_AGENT_RUN_NAME).getConnection();
				PreparedStatement stmnt = conn.prepareStatement(sql);) {

			try {
				conn.setAutoCommit(false);

				for (ExtractionRunDetails row : this.extractionRun.getDetails()) {

					executeSavePacketRowSQL(stmnt, this.extractionRun.getId(), row.getId(), row.getRelType(),
							row.getRelId(), row.getCId(), row.getAccountNo(), row.getActionId(),
							row.getFile().getPath(), row.getStatus(), row.getFailureReason());

					for (File file : row.getAttachments()) {
						executeSavePacketRowSQL(stmnt, this.extractionRun.getId(), row.getId(), row.getRelType(),
								row.getRelId(), row.getCId(), row.getAccountNo(), row.getActionId(), file.getPath(),
								row.getStatus(), row.getFailureReason());
					}

					conn.commit();
					conn.setAutoCommit(true);
				}
			}
			catch (SQLException e) {
				conn.rollback();
				throw e;
			}
			finally {
				conn.setAutoCommit(true);
			}
		}
		catch (SQLException e) {
			throw new SavePacketException(e);
		}
	}

	private void saveRun() throws SaveRunException {
		info("Saving run to database...");

		if(isTestMode())
			return;

		String sql = this.SQL_SAVE_RUN.replaceAll(this.SQL_TABLE_NAME_SUBST_VARIABLE, getConfig(CONFIG_EXTRACTION_RUN_TABLE));

		try {
			executeSaveRunSQL(sql);
		}
		catch(SQLException e) {
			throw new SaveRunException(e);
		}
	}
	
	private void executeSavePacketRowSQL(PreparedStatement stmnt, int id, String docId, String relType, String relId, 
											String cId, String accountNo, String actionId, String path, String status, String reason) throws SQLException  {
		
		int i = 1;
		stmnt.setInt(	i++, id);
		stmnt.setString(i++, docId);
		stmnt.setString(i++, relType);
		stmnt.setString(i++, relId);
		stmnt.setString(i++, cId);
		stmnt.setString(i++, accountNo);
		stmnt.setString(i++, actionId);
		stmnt.setString(i++, path);
		stmnt.setString(i++, status);
		stmnt.setString(i++, reason);

		stmnt.executeUpdate();
	}

	private void executeSaveRunSQL(String sql) throws SQLException {
		try (Connection conn = getDBAgent(this.DB_AGENT_RUN_NAME).getConnection();
				PreparedStatement stmnt = conn.prepareStatement(sql);) {

			int i = 1;
			stmnt.setInt(i++, this.extractionRun.getRowsProcessed());
			stmnt.setInt(i++, this.extractionRun.getDocumentsExtracted());
			stmnt.setTimestamp(i++, this.extractionRun.getProcessedUpTo());
			stmnt.setString(i++, this.extractionRun.getStatus());
			stmnt.setString(i++, this.extractionRun.getDuration());
			stmnt.setInt(i++, this.extractionRun.getId());

			stmnt.executeUpdate();
		}
	}
	
	private void deleteRunFromDatabase() {
		if(isTestMode())
			return;

		String sql = this.SQL_DELETE_RUN.replaceAll(this.SQL_TABLE_NAME_SUBST_VARIABLE, getConfig(CONFIG_EXTRACTION_RUN_TABLE));

		try (Connection conn = getDBAgent(this.DB_AGENT_RUN_NAME).getConnection();
				PreparedStatement stmnt = conn.prepareStatement(sql)) {
			
			int i = 1;
			stmnt.setInt(i++, this.extractionRun.getId());

			stmnt.executeUpdate();
		}
		catch(SQLException e) {
		}
	}

	private void initializeDBAgents() {
		this.logger.info("Initialize database agents.");

		this.dbAgents = new Hashtable<String, DatabaseAgent>();
		
		this.dbAgents.put(this.DB_AGENT_SOURCE_NAME, new DatabaseAgent(getConfig(CONFIG_SERVER), getConfig(CONFIG_DATABASE)));
		
		if(!getConfig(this.CONFIG_LOG_RUN_SERVER).equalsIgnoreCase(getConfig(this.CONFIG_SERVER)) ||
				!getConfig(this.CONFIG_LOG_RUN_DATABASE).equalsIgnoreCase(getConfig(this.CONFIG_DATABASE)))
			this.dbAgents.put(this.DB_AGENT_RUN_NAME, new DatabaseAgent(getConfig(CONFIG_LOG_RUN_SERVER), getConfig(CONFIG_LOG_RUN_DATABASE)));
		else
			this.dbAgents.put(this.DB_AGENT_RUN_NAME, this.dbAgents.get(this.DB_AGENT_SOURCE_NAME));
	}
	
	private void initializeExtractionRootDir(){
		this.extractionFolder = 
				getParam(this.PARAM_OUTPUT_DIR) + "/" + getConfig(this.CONFIG_EXTRACTION_ROOT_DIR);

		if(!shouldInitialize() || isTestMode())
			return;

		Path folder = Paths.get(this.extractionFolder); 
		
		info("Initialize output folder.");

		deleteFolder(folder);
	}

	private void deleteFolder(Path path){
		info("Deleting folder " + path.toString());
	    
		try (Stream<Path> paths = Files.walk(path)) {
	        paths.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
	    }
	    catch (IOException e) {
		}
	}

	private void initializeRunTables() throws SQLException {
		if (isTestMode()) 
			return;

			try (Connection conn = getDBAgent(this.DB_AGENT_RUN_NAME).getConnection()){

			if (shouldInitialize()) {
				
				info("Drop extraction tables.");
			    
				executeDDL(conn, this.SQL_DROP_TABLE.replaceAll(SQL_TABLE_NAME_SUBST_VARIABLE, getConfig(CONFIG_EXTRACTION_RUN_DETAILS_TABLE)));
				executeDDL(conn, this.SQL_DROP_TABLE.replaceAll(this.SQL_TABLE_NAME_SUBST_VARIABLE, getConfig(CONFIG_EXTRACTION_RUN_TABLE)));
			}

			info("Create extraction tables (if they are not exist).");
			
			executeDDL(conn, this.SQL_CREATE_RUN_TABLE.replaceAll(this.SQL_TABLE_NAME_SUBST_VARIABLE, getConfig(CONFIG_EXTRACTION_RUN_TABLE)));
			executeDDL(conn, this.SQL_CREATE_RUN_DETAILS_TABLE.replaceAll(this.SQL_TABLE_NAME_SUBST_VARIABLE, getConfig(CONFIG_EXTRACTION_RUN_DETAILS_TABLE))
														.replaceAll(SQL_REF_TABLE_NAME_SUBST_VARIABLE, getConfig(CONFIG_EXTRACTION_RUN_TABLE)));
		}
	}
	
	private void executeDDL(Connection conn, String sql) throws SQLException {
		try (Statement stmnt = conn.createStatement()){
			stmnt.execute(sql);
		}
	}

	private Timestamp getLastProcessedDate() throws SQLException {
		String logMsg = "Retrieving last processed date... ";
		
		Timestamp date = null;
		
		String sql = this.SQL_MAX_PROCESSED_DATE.replaceAll(this.SQL_TABLE_NAME_SUBST_VARIABLE, getConfig(CONFIG_EXTRACTION_RUN_TABLE));
		
		try (Connection conn = getDBAgent(this.DB_AGENT_RUN_NAME).getConnection(); 
				Statement stmnt = conn.createStatement()){

			ResultSet resultSet = stmnt.executeQuery(sql);
			
			date = resultSet.next() ? resultSet.getTimestamp(1) : null;

			logMsg += date != null ? "Last processed date: " + new SimpleDateFormat("d/M/yyyy H:m:s.S").format(date): "No last processed date found.";
			
			info(logMsg);
			
			return date;
		}
	}
	
	private void logFile(String msgStart, int docNo, String msgEnd, boolean asWarning) {
		String docNoStr = " #" + docNo + StringUtils.SPACE;
		
		if(asWarning) {
			this.docloggerStart.warn(msgStart);
			this.docloggerDocNo.warn(docNoStr);
			this.docloggerEnd.warn(msgEnd);
		}
		else {
			this.docloggerStart.info(msgStart);
			this.docloggerDocNo.info(docNoStr);
			this.docloggerEnd.info(msgEnd);
		}
	}
	
	private void logNewLine() {
		logNewLine(1);
	}
	
	private void logNewLine(int numberOfLines) {
		for(int i = 0; i < numberOfLines; i++) {
			this.newLineLogger.info(StringUtils.SPACE);
		}
	}
	
	private void info(String msg) {
		this.logger.info(msg); 
	}
	
	private void warn(String msg) {
		this.logger.warn(msg);
	}
	
	private void error(String msg) {
		this.logger.error(msg);
	}
	
	private void error(String msg, Throwable t) {
		this.logger.error(msg, t);
	}
	
	private boolean shouldInitialize() {
		if (this.shouldInitialize == null) {
			File f = new File("init.txt");
			this.shouldInitialize = Boolean.parseBoolean(getParam(PARAM_NAME_INITIALIZE)) && f.exists();
			f.delete();
		}

		return this.shouldInitialize;
	}
	
	private boolean isTestMode() {
		return Boolean.parseBoolean(getParam(PARAM_NAME_TEST_MODE)); 
	}
	
	private boolean extractAttachemts() {
		return Boolean.parseBoolean(getParam(PARAM_NAME_EXTRACT_ATTACHMENTS)); 
	}
	
	private void deletePacketFiles(String reason) {
		info("Deleting packet's files...");
		for(ExtractionRunDetails detail : this.extractionRun.getDetails()) {
			detail.markAsFailed(reason);
			deleteFile(detail.getFile(), true);
			for(File file : detail.getAttachments()) {
				deleteFile(file, true);
			}
		}
	}

	private void deleteFile(File file, boolean deleteParentDirs) {
		file.delete();
		
		if(!deleteParentDirs)
			return;
		
		Path extractionRoot = Paths.get(this.extractionFolder);
		
		File parent = file.getParentFile();

		while(!parent.toPath().equals(extractionRoot)) {
			parent.delete();
			parent = parent.getParentFile();
		}
	}
	
	private boolean stopExecution() {
		if(!this.stopExecution) {
			File f = new File("stop.txt");
			this.stopExecution = f.exists();
			f.delete();
			if(stopExecution)
				info("Execution stop request received.");
		}

		return stopExecution;
	}

	private boolean runPeriodExpired() {
		if(!this.runPeriodExpired) {
			this.runPeriodExpired = !LocalDateTime.now().isBefore(this.endTime);
			if(runPeriodExpired)
				info("The specified run period has been expired. Requested run period: " + this.extractionRun.getRunFor().toLowerCase() + ".");
		}
		
		return runPeriodExpired;
	}
}
