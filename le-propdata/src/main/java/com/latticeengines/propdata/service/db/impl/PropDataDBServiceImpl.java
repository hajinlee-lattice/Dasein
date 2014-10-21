package com.latticeengines.propdata.service.db.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.CommandIds;
import com.latticeengines.domain.exposed.propdata.Commands;
import com.latticeengines.propdata.entitymanager.PropDataEntityMgr;
import com.latticeengines.propdata.service.db.PropDataContext;
import com.latticeengines.propdata.service.db.PropDataDBService;
import com.latticeengines.propdata.service.db.PropDataJobService;
import com.latticeengines.propdata.service.db.PropDataKey;
import com.latticeengines.scheduler.exposed.fairscheduler.LedpQueueAssigner;

@Component
public class PropDataDBServiceImpl implements PropDataDBService {

	private static final int MATCHING_TIMEOUT_MINUTES = 60;

	private final Log log = LogFactory.getLog(this.getClass());

	static final String PROPDATA_OUTPUT = "propdata_output";
	static final String PROPDATA_INPUT = "propdata_input";

	@Autowired
	private PropDataEntityMgr propDataEntityMgr;

	@Autowired
	private PropDataJobService propDataJobService;

	@Autowired
	protected Configuration yarnConfiguration;

	@Value("${propdata.datasource.url}")
	private String jdbcUrl;
	@Value("${propdata.datasource.user}")
	private String jdbcUser;
	@Value("${propdata.datasource.password.encrypted}")
	private String jdbcPassword;

	@Value("${dataplatform.customer.basedir}")
	private String customerBaseDir;

	@Override
	public PropDataContext importFromDB(PropDataContext requestContext) {
		PropDataContext responseContext = new PropDataContext();

		String tableName = requestContext.getProperty(
				PropDataKey.ImportExportKey.TABLE.getValue(), String.class);
		String customer = requestContext.getProperty(
				PropDataKey.ImportExportKey.CUSTOMER.getValue(), String.class);
		StringBuilder applicationIds = new StringBuilder();
		StringBuilder newTables = new StringBuilder();

		try {

			List<String> tableList = new ArrayList<>();
			List<String> keyColsList = new ArrayList<>();

			generateNewTables(requestContext, tableList, keyColsList);

			String assignedQueue = LedpQueueAssigner
					.getMRQueueNameForSubmission();
			for (int i = 0; i < tableList.size(); i++) {
				String newTable = tableList.get(i);
				String keyCols = keyColsList.get(i);
				ApplicationId appId = propDataJobService.importData(
						newTable,
						getDataHdfsPath(customer, tableName + "/" + newTable,
								PROPDATA_OUTPUT), assignedQueue, customer,
						keyCols, getConnectionString());
				Integer applicationId;
				if (appId != null) {
					applicationId = appId.getId();
				} else {
					return responseContext;
				}
				applicationIds.append(applicationId);
				newTables.append(newTable);

			}
			applicationIds.setLength(applicationIds.length() - 1);
			responseContext.setProperty(
					PropDataKey.ImportExportKey.APPLICATION_ID.getValue(),
					applicationIds.toString());
			responseContext.setProperty(
					PropDataKey.ImportExportKey.CUSTOMER.getValue(), customer);
			responseContext.setProperty(
					PropDataKey.ImportExportKey.TABLE.getValue(),
					newTables.toString());

			log.info("Import job response =" + responseContext);

		} catch (Exception ex) {
			log.error("Failed to import!", ex);
		}

		return responseContext;
	}

	@Override
	public PropDataContext createSingleAVROFromTable(
			PropDataContext requestContext) {
		PropDataContext responseContext = new PropDataContext();

		String tableName = requestContext.getProperty(
				PropDataKey.ImportExportKey.TABLE.getValue(), String.class);
		String customer = requestContext.getProperty(
				PropDataKey.ImportExportKey.CUSTOMER.getValue(), String.class);
		String keyCols = requestContext.getProperty(
				PropDataKey.ImportExportKey.KEY_COLS.getValue(), String.class);

		try {

			String assignedQueue = LedpQueueAssigner
					.getMRQueueNameForSubmission();
			ApplicationId appId = propDataJobService.importData(tableName,
					getDataHdfsPath(customer, tableName, PROPDATA_OUTPUT),
					assignedQueue, customer, keyCols, getConnectionString());
			Integer applicationId;
			if (appId != null) {
				applicationId = appId.getId();
			} else {
				return responseContext;
			}

			responseContext.setProperty(
					PropDataKey.ImportExportKey.APPLICATION_ID.getValue(),
					applicationId + "");
			responseContext.setProperty(
					PropDataKey.ImportExportKey.CUSTOMER.getValue(), customer);
			responseContext.setProperty(
					PropDataKey.ImportExportKey.TABLE.getValue(), tableName);

			log.info("Import job response =" + responseContext);

		} catch (Exception ex) {
			log.error("Failed to import!", ex);
		}

		return responseContext;
	}

	private void generateNewTables(PropDataContext requestContext,
			List<String> tableList, List<String> keyColsList) {

		Long commandId = requestContext.getProperty(
				PropDataKey.CommandsKey.COMMAND_ID.getValue(), Long.class);
		String destTablesStr = requestContext.getProperty(
				PropDataKey.CommandsKey.DESTTABLES.getValue(), String.class);
		String commandName = requestContext.getProperty(
				PropDataKey.CommandsKey.COMMAND_NAME.getValue(), String.class);

		if (StringUtils.isEmpty(destTablesStr)) {
			return;
		}
		String[] destTables = StringUtils.split(destTablesStr.trim(), "|");
		for (String destTable : destTables) {
			generateNewTable(commandId, destTable, commandName, tableList,
					keyColsList);
		}
	}

	private void generateNewTable(Long commandId, String destTable,
			String commandName, List<String> tableList, List<String> keyColsList) {

		StringBuilder builder = new StringBuilder();
		builder.append(commandName).append("_").append(commandId).append("_")
				.append(destTable);
		tableList.add(builder.toString());
		keyColsList.add("Source_Id");
		tableList.add(builder.append("_MetaData").toString());
		keyColsList.add("InternalColumnName");
	}

	@Override
	public PropDataContext exportToDB(PropDataContext requestContext) {
		PropDataContext responseContext = new PropDataContext();

		try {

			createSingleTableFromAvro(requestContext);
			return exportFromHdfsToDB(requestContext);

		} catch (Exception ex) {
			log.error("Failed to export!", ex);
		}

		return responseContext;
	}

	@Override
	public PropDataContext addCommandAndWaitForComplete(
			PropDataContext requestContext) {

		PropDataContext responseContext = new PropDataContext();
		Commands commands = convertToCommands(requestContext);

		propDataEntityMgr.createCommands(commands);

		boolean isComplete = waitForComplete(commands);
		if (!isComplete) {
			return responseContext;
		}

		responseContext.setProperty(
				PropDataKey.CommandsKey.COMMAND_ID.getValue(),
				commands.getPid());
		responseContext.setProperty(PropDataKey.CommandsKey.DESTTABLES
				.getValue(), requestContext.getProperty(
				PropDataKey.CommandsKey.DESTTABLES.getValue(), String.class));
		responseContext.setProperty(PropDataKey.CommandsKey.COMMAND_NAME
				.getValue(), requestContext.getProperty(
				PropDataKey.CommandsKey.COMMAND_NAME.getValue(), String.class));
		return responseContext;
	}

	private boolean waitForComplete(Commands commands) {

		Long commandId = null;
		Date startTime = new Date(System.currentTimeMillis());
		commandId = commands.getPid();
		while (true) {
			Commands newCommands = propDataEntityMgr.getCommands(commandId);
			if (newCommands.getCommandStatus() == 3) {
				break;
			}
			try {
				Thread.sleep(10000L);
			} catch (InterruptedException e) {
				log.warn("Thread sleep was interrupted!");
			}

			Date endTime = new Date(System.currentTimeMillis());
			if (DateUtils.addMinutes(startTime, MATCHING_TIMEOUT_MINUTES)
					.before(endTime)) {
				log.error("CommandId="
						+ commandId
						+ " has run more than 60 minutes or failed. CommandStatus="
						+ newCommands.getCommandStatus());
				return false;
			}

			long duration = endTime.getTime() - startTime.getTime();
			long minutes = TimeUnit.MILLISECONDS.toMinutes(duration);

			log.info("CommandId=" + commandId + " has already run for minutes="
					+ minutes);
		}
		return true;
	}

	@Override
	public void createSingleTableFromAvro(PropDataContext requestContext)
			throws Exception {

		String tableName = requestContext.getProperty(
				PropDataKey.ImportExportKey.TABLE.getValue(), String.class);

		dropTable(tableName);

		String sql = generateSql(requestContext);
		propDataEntityMgr.createTableByQuery(sql);

	}

	private String generateSql(PropDataContext requestContext) throws Exception {
		String customer = requestContext.getProperty(
				PropDataKey.ImportExportKey.CUSTOMER.getValue(), String.class);
		String table = requestContext.getProperty(
				PropDataKey.ImportExportKey.TABLE.getValue(), String.class);

		String inputDir = getDataHdfsPath(customer, table, PROPDATA_INPUT);
		List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration,
				inputDir, new HdfsFilenameFilter() {
					@Override
					public boolean accept(String filename) {

						return filename.endsWith(".avro");
					}
				});

		return generateSqlFromAvroFile(files.get(0), table);
	}

	private String generateSqlFromAvroFile(String file, String table) {
		StringBuilder builder = new StringBuilder();
		Schema schema = AvroUtils.getSchema(yarnConfiguration, new Path(file));
		builder.append("CREATE TABLE ").append(table).append(" (");

		List<Field> fields = schema.getFields();
		for (Field field : fields) {
			addField(builder, field);
		}
		builder.setLength(builder.length() - 1);
		builder.append(")");

		return builder.toString();
	}

	private void addField(StringBuilder builder, Field field) {
		builder.append(field.name()).append(" ");

		Schema schema = field.schema();
		List<Schema> types = schema.getTypes();

		for (Schema type : types) {
			Type avroType = type.getType();
			String mappedType = AvroToDBTypeMapper.getType(avroType);
			builder.append(mappedType);
			switch (mappedType) {
			case "VARCHAR":
			case "VARBINARY":
				String length = field.getProp("length");
				if (length == null || length.equals("0")) {
					builder.append("(max)");
				} else {
					builder.append("(").append(length).append(")");
				}
			}
			builder.append(" ");
		}
		builder.append(",");
	}

	private void dropTable(String tableName) {
		try {
			propDataEntityMgr.dropTable(tableName);
		} catch (Exception ex) {
			log.warn("Exception where drop table=" + tableName + " Message="
					+ ex.getMessage());
		}
	}

	private PropDataContext exportFromHdfsToDB(PropDataContext requestContext) {
		PropDataContext responseContext = new PropDataContext();

		String customer = requestContext.getProperty(
				PropDataKey.ImportExportKey.CUSTOMER.getValue(), String.class);
		String table = requestContext.getProperty(
				PropDataKey.ImportExportKey.TABLE.getValue(), String.class);
		String keyCols = requestContext.getProperty(
				PropDataKey.ImportExportKey.KEY_COLS.getValue(), String.class);
		String assignedQueue = LedpQueueAssigner.getMRQueueNameForSubmission();

		int applicationId = propDataJobService.exportData(table,
				getDataHdfsPath(customer, table, PROPDATA_INPUT),
				assignedQueue, customer, keyCols, getConnectionString())
				.getId();

		responseContext.setProperty(
				PropDataKey.ImportExportKey.APPLICATION_ID.getValue(),
				applicationId);
		responseContext.setProperty(
				PropDataKey.ImportExportKey.CUSTOMER.getValue(), customer);
		responseContext.setProperty(
				PropDataKey.ImportExportKey.TABLE.getValue(), table);

		log.info("Import job response =" + responseContext);

		return responseContext;

	}

	String getDataHdfsPath(String customer, String table, String inputOutputDir) {
		return customerBaseDir + "/" + customer + "/data/" + inputOutputDir
				+ "/" + table;
	}

	private Commands convertToCommands(PropDataContext requestContext) {

		CommandIds commandIds = new CommandIds();
		commandIds.setCreatedBy(requestContext.getProperty(
				PropDataKey.CommandIdsKey.CREATED_BY.getValue(), String.class));
		Date now = new Date();
		commandIds.setCreateTime(now);

		Commands commands = new Commands();
		commands.setCommandIds(commandIds);

		commands.setCommandName(requestContext.getProperty(
				PropDataKey.CommandsKey.COMMAND_NAME.getValue(), String.class));
		commands.setCommandStatus(0);
		commands.setContractExternalID(requestContext.getProperty(
				PropDataKey.CommandsKey.CONTRACT_EXTERNAL_ID.getValue(),
				String.class));
		commands.setCreateTime(now);
		commands.setDeploymentExternalID(requestContext.getProperty(
				PropDataKey.CommandsKey.DEPLOYMENT_EXTERNAL_ID.getValue(),
				String.class));
		commands.setDestTables(requestContext.getProperty(
				PropDataKey.CommandsKey.DESTTABLES.getValue(), String.class));

		Boolean isDownloading = requestContext.getProperty(
				PropDataKey.CommandsKey.IS_DOWNLOADING.getValue(),
				Boolean.class);
		if (isDownloading != null) {
			commands.setIsDownloading(isDownloading);
		} else {
			commands.setIsDownloading(false);
		}

		Integer maxNumRetries = requestContext.getProperty(
				PropDataKey.CommandsKey.MAX_NUMR_ETRIES.getValue(),
				Integer.class);
		if (maxNumRetries != null) {
			commands.setMaxNumRetries(maxNumRetries);
		} else {
			commands.setMaxNumRetries(5);
		}

		commands.setNumRetries(0);
		commands.setProcessUID(UUID.randomUUID().toString());
		commands.setSourceTable(requestContext.getProperty(
				PropDataKey.CommandsKey.SOURCE_TABLE.getValue(), String.class));
		return commands;
	}

	private String getConnectionString() {

		String driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		try {
			Class.forName(driverClass);
		} catch (ClassNotFoundException e) {
			throw new LedpException(LedpCode.LEDP_11000, e,
					new String[] { driverClass });
		}
		return jdbcUrl + "user=" + jdbcUser + ";password=" + jdbcPassword;
	}
}
