package com.latticeengines.propdata.eai.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
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
import com.latticeengines.dataplatform.exposed.service.SqoopSyncJobService;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.propdata.eai.entitymanager.PropDataEntityMgr;
import com.latticeengines.propdata.eai.service.PropDataContext;
import com.latticeengines.propdata.eai.service.PropDataDBService;
import com.latticeengines.propdata.eai.service.PropDataKey.CommandsKey;
import com.latticeengines.propdata.eai.service.PropDataKey.ImportExportKey;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component
public class PropDataDBServiceImpl implements PropDataDBService {

    private static final int MATCHING_TIMEOUT_MINUTES = 120;

    private final Log log = LogFactory.getLog(this.getClass());

    static final String PROPDATA_OUTPUT = "propdata_output";
    static final String PROPDATA_INPUT = "propdata_input";

    @Autowired
    private PropDataEntityMgr propDataEntityMgr;

    @Autowired
    private SqoopSyncJobService propDataJobService;

    @Autowired
    protected Configuration yarnConfiguration;

    @Value("${propdata.datasource.url}")
    private String jdbcUrl;
    @Value("${propdata.datasource.host}")
    private String jdbcHost;
    @Value("${propdata.datasource.port}")
    private String jdbcPort;
    @Value("${propdata.datasource.dbname}")
    private String jdbcDb;
    @Value("${propdata.datasource.type}")
    private String jdbcType;
    @Value("${propdata.datasource.user}")
    private String jdbcUser;
    @Value("${propdata.datasource.password.encrypted}")
    private String jdbcPassword;

    @Value("${dataplatform.customer.basedir}")
    private String customerBaseDir;

    @Override
    public PropDataContext importFromDB(PropDataContext requestContext) {
        PropDataContext responseContext = new PropDataContext();

        String tableName = requestContext.getProperty(ImportExportKey.TABLE.getKey(), String.class);
        String customer = requestContext.getProperty(ImportExportKey.CUSTOMER.getKey(), String.class);
        StringBuilder applicationIds = new StringBuilder();
        StringBuilder newTables = new StringBuilder();

        try {

            List<String> tableList = new ArrayList<>();
            List<String> keyColsList = new ArrayList<>();

            generateNewTables(requestContext, tableList, keyColsList);

            String assignedQueue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
            DbCreds.Builder builder = new DbCreds.Builder();
            builder.host(jdbcHost).port(Integer.parseInt(jdbcPort)).db(jdbcDb)
                    .user(jdbcUser).password(jdbcPassword).dbType(jdbcType);
            DbCreds creds = new DbCreds(builder);
            for (int i = 0; i < tableList.size(); i++) {
                String newTable = tableList.get(i);
                String keyCols = keyColsList.get(i);
                ApplicationId appId = propDataJobService.importDataSync(newTable,
                        getDataHdfsPath(customer, tableName + "/" + newTable, PROPDATA_OUTPUT), creds, assignedQueue,
                        customer, Arrays.asList(keyCols), "", 0);
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
            responseContext.setProperty(ImportExportKey.APPLICATION_ID.getKey(), applicationIds.toString());
            responseContext.setProperty(ImportExportKey.CUSTOMER.getKey(), customer);
            responseContext.setProperty(ImportExportKey.TABLE.getKey(), newTables.toString());

            log.info("Import job response =" + responseContext);

        } catch (Exception ex) {
            log.error("Failed to import!", ex);
        }

        return responseContext;
    }

    @Override
    public PropDataContext createSingleAVROFromTable(PropDataContext requestContext) {
        PropDataContext responseContext = new PropDataContext();

        String tableName = requestContext.getProperty(ImportExportKey.TABLE.getKey(), String.class);
        String customer = requestContext.getProperty(ImportExportKey.CUSTOMER.getKey(), String.class);
        String keyCols = requestContext.getProperty(ImportExportKey.KEY_COLS.getKey(), String.class);

        try {
            DbCreds.Builder builder = new DbCreds.Builder();
            builder.host(jdbcHost).port(Integer.parseInt(jdbcPort)).db(jdbcDb)
                    .user(jdbcUser).password(jdbcPassword).dbType(jdbcType);
            DbCreds creds = new DbCreds(builder);
            String assignedQueue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
            ApplicationId appId = propDataJobService.importDataSync(tableName,
                    getDataHdfsPath(customer, tableName, PROPDATA_OUTPUT), creds, assignedQueue, customer, Arrays.asList(keyCols),
                    "", 1);
            Integer applicationId;
            if (appId != null) {
                applicationId = appId.getId();
            } else {
                return responseContext;
            }

            responseContext.setProperty(ImportExportKey.APPLICATION_ID.getKey(), applicationId + "");
            responseContext.setProperty(ImportExportKey.CUSTOMER.getKey(), customer);
            responseContext.setProperty(ImportExportKey.TABLE.getKey(), tableName);

            log.info("Import job response =" + responseContext);

        } catch (Exception ex) {
            log.error("Failed to import!", ex);
        }

        return responseContext;
    }

    private void generateNewTables(PropDataContext requestContext, List<String> tableList, List<String> keyColsList) {

        Long commandId = requestContext.getProperty(CommandsKey.COMMAND_ID.getKey(), Long.class);
        String destTablesStr = requestContext.getProperty(CommandsKey.DESTTABLES.getKey(), String.class);
        String commandName = requestContext.getProperty(CommandsKey.COMMAND_NAME.getKey(), String.class);

        if (StringUtils.isEmpty(destTablesStr)) {
            return;
        }
        String[] destTables = StringUtils.split(destTablesStr.trim(), "|");
        for (String destTable : destTables) {
            generateNewTable(requestContext, commandId, destTable.trim(), commandName, tableList, keyColsList);
        }
    }

    private void generateNewTable(PropDataContext requestContext, Long commandId, String destTable, String commandName,
            List<String> tableList, List<String> keyColsList) {

        StringBuilder builder = new StringBuilder();
        builder.append(commandName).append("_").append(commandId).append("_").append(destTable);
        tableList.add(builder.toString());
        String keyCols = requestContext.getProperty(ImportExportKey.KEY_COLS.getKey(), String.class);
        keyColsList.add("Source_" + keyCols);
        tableList.add(builder.append("_MetaData").toString());
        keyColsList.add("InternalColumnName");
    }

    @Override
    public PropDataContext exportToDB(PropDataContext requestContext) {

        PropDataContext responseContext = new PropDataContext();
        Boolean mapColumn = requestContext.getProperty(ImportExportKey.MAP_COLUMN.getKey(), Boolean.class);
        if (mapColumn == null) {
            mapColumn = true;
        }

        try {

            List<StringBuilder> mapColumnSqls = new ArrayList<>();
            internalCreateTableFromAvro(requestContext, mapColumnSqls);
            responseContext = exportFromHdfsToDB(requestContext);

            if (mapColumn) {
                for (StringBuilder mapColumnSql : mapColumnSqls) {
                    propDataEntityMgr.executeProcedure(mapColumnSql.toString());
                }
            }

        } catch (Exception ex) {
            log.error("Failed to export!", ex);
        }

        return responseContext;
    }

//    @Override
//    public PropDataContext addCommandAndWaitForComplete(PropDataContext requestContext) {
//
//        PropDataContext responseContext = new PropDataContext();
//        Commands commands = convertToCommands(requestContext);
//
//        propDataEntityMgr.createCommands(commands);
//
//        boolean isComplete = waitForComplete(commands);
//        if (!isComplete) {
//            return responseContext;
//        }
//
//        responseContext.setProperty(CommandsKey.COMMAND_ID.getKey(), commands.getPid());
//        responseContext.setProperty(CommandsKey.DESTTABLES.getKey(),
//                requestContext.getProperty(CommandsKey.DESTTABLES.getKey(), String.class));
//        responseContext.setProperty(CommandsKey.COMMAND_NAME.getKey(),
//                requestContext.getProperty(CommandsKey.COMMAND_NAME.getKey(), String.class));
//        return responseContext;
//    }
//
//    private boolean waitForComplete(Commands commands) {
//
//        Long commandId = null;
//        Date startTime = new Date(System.currentTimeMillis());
//        commandId = commands.getPid();
//        while (true) {
//            Commands newCommands = propDataEntityMgr.getCommands(commandId);
//            if (newCommands.getCommandStatus() == 3) {
//                break;
//            }
//            try {
//                Thread.sleep(10000L);
//            } catch (InterruptedException e) {
//                log.warn("Thread sleep was interrupted!");
//            }
//
//            Date endTime = new Date(System.currentTimeMillis());
//            if (DateUtils.addMinutes(startTime, MATCHING_TIMEOUT_MINUTES).before(endTime)) {
//                log.error("CommandId=" + commandId + " has run more than 60 minutes or failed. CommandStatus="
//                        + newCommands.getCommandStatus());
//                return false;
//            }
//
//            long duration = endTime.getTime() - startTime.getTime();
//            long minutes = TimeUnit.MILLISECONDS.toMinutes(duration);
//
//            log.info("CommandId=" + commandId + " has already run for minutes=" + minutes);
//        }
//        return true;
//    }

    @Override
    public void createSingleTableFromAvro(PropDataContext requestContext) throws Exception {

        List<StringBuilder> mapColumnSqls = new ArrayList<>();
        internalCreateTableFromAvro(requestContext, mapColumnSqls);

    }

    private void internalCreateTableFromAvro(PropDataContext requestContext, List<StringBuilder> mapColumnSqls)
            throws Exception {
        String tableName = requestContext.getProperty(ImportExportKey.TABLE.getKey(), String.class);

        StringBuilder createTableSql = new StringBuilder();

        generateSql(requestContext, createTableSql, mapColumnSqls);

        dropTable(tableName);
        propDataEntityMgr.executeQueryUpdate(createTableSql.toString());
    }

    private void generateSql(PropDataContext requestContext, StringBuilder createTableSql,
            List<StringBuilder> mapColumnSqls) throws Exception {
        String customer = requestContext.getProperty(ImportExportKey.CUSTOMER.getKey(), String.class);
        String table = requestContext.getProperty(ImportExportKey.TABLE.getKey(), String.class);

        String inputDir = getDataHdfsPath(customer, table, PROPDATA_INPUT);
        List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, inputDir, new HdfsFilenameFilter() {
            @Override
            public boolean accept(String filename) {

                return filename.endsWith(".avro");
            }
        });

        generateSqlFromAvroFile(files.get(0), table, createTableSql, mapColumnSqls);
    }

    private void generateSqlFromAvroFile(String file, String table, StringBuilder createTableSql,
            List<StringBuilder> mapColumnSqls) {
        Schema schema = AvroUtils.getSchema(yarnConfiguration, new Path(file));

        getCrearteTableSql(createTableSql, table, schema);
        getMapColumnSql(mapColumnSqls, table, schema);

    }

    private void getMapColumnSql(List<StringBuilder> mapColumnSqls, String table, Schema schema) {

        List<Field> fields = schema.getFields();
        for (Field field : fields) {
            StringBuilder builder = addFieldForMapColumn(field, table);
            if (builder != null && builder.length() > 0) {
                mapColumnSqls.add(builder);
            }
        }
    }

    private StringBuilder addFieldForMapColumn(Field field, String table) {

        String fieldName = field.name();
        if (fieldName.equalsIgnoreCase("Company")) {
            StringBuilder builder = new StringBuilder("sp_rename ").append("'").append(table).append(".")
                    .append(fieldName).append("', ");
            builder.append("'").append("Name").append("',").append(" 'COLUMN'");
            return builder;
        }
        return null;
    }

    private void getCrearteTableSql(StringBuilder createTableSql, String table, Schema schema) {
        createTableSql.append("CREATE TABLE ").append(table).append(" (");
        List<Field> fields = schema.getFields();
        for (Field field : fields) {
            addFieldForCreateTable(createTableSql, field);
        }
        createTableSql.setLength(createTableSql.length() - 1);
        createTableSql.append(")");
    }

    private void addFieldForCreateTable(StringBuilder builder, Field field) {
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
            log.warn("Exception where drop table=" + tableName + " Message=" + ex.getMessage());
        }
    }

    private PropDataContext exportFromHdfsToDB(PropDataContext requestContext) {
        PropDataContext responseContext = new PropDataContext();

        String customer = requestContext.getProperty(ImportExportKey.CUSTOMER.getKey(), String.class);
        String table = requestContext.getProperty(ImportExportKey.TABLE.getKey(), String.class);
        String assignedQueue = LedpQueueAssigner.getPropDataQueueNameForSubmission();

        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(jdbcHost).port(Integer.parseInt(jdbcPort)).db(jdbcDb)
                .user(jdbcUser).password(jdbcPassword).dbType(jdbcType);
        DbCreds creds = new DbCreds(builder);

        Integer appId = propDataJobService.exportDataSync(table, getDataHdfsPath(customer, table, PROPDATA_INPUT),
                creds, assignedQueue, customer, 1, null).getId();

        Integer applicationId;
        if (appId != null) {
            applicationId = appId;
        } else {
            return responseContext;
        }

        responseContext.setProperty(ImportExportKey.APPLICATION_ID.getKey(), applicationId);
        responseContext.setProperty(ImportExportKey.CUSTOMER.getKey(), customer);
        responseContext.setProperty(ImportExportKey.TABLE.getKey(), table);

        log.info("Import job response =" + responseContext);

        return responseContext;

    }

    String getDataHdfsPath(String customer, String table, String inputOutputDir) {
        return customerBaseDir + "/" + customer + "/data/" + inputOutputDir + "/" + table;
    }

//    private Commands convertToCommands(PropDataContext requestContext) {
//
//        CommandIds commandIds = new CommandIds();
//        commandIds.setCreatedBy(requestContext.getProperty(CommandIdsKey.CREATED_BY.getKey(), String.class));
//        Date now = new Date();
//        commandIds.setCreateTime(now);
//
//        Commands commands = new Commands();
//        commands.setCommandIds(commandIds);
//
//        commands.setCommandName(requestContext.getProperty(CommandsKey.COMMAND_NAME.getKey(), String.class));
//        commands.setCommandStatus(0);
//        commands.setContractExternalID(requestContext.getProperty(CommandsKey.CONTRACT_EXTERNAL_ID.getKey(),
//                String.class));
//        commands.setCreateTime(now);
//        commands.setDeploymentExternalID(requestContext.getProperty(CommandsKey.DEPLOYMENT_EXTERNAL_ID.getKey(),
//                String.class));
//        commands.setDestTables(requestContext.getProperty(CommandsKey.DESTTABLES.getKey(), String.class));
//
//        Boolean isDownloading = requestContext.getProperty(CommandsKey.IS_DOWNLOADING.getKey(), Boolean.class);
//        if (isDownloading != null) {
//            commands.setIsDownloading(isDownloading);
//        } else {
//            commands.setIsDownloading(false);
//        }
//
//        Integer maxNumRetries = requestContext.getProperty(CommandsKey.MAX_NUMR_ETRIES.getKey(), Integer.class);
//        if (maxNumRetries != null) {
//            commands.setMaxNumRetries(maxNumRetries);
//        } else {
//            commands.setMaxNumRetries(5);
//        }
//
//        commands.setNumRetries(0);
//        commands.setProcessUID(UUID.randomUUID().toString());
//        String sourceTable = requestContext.getProperty(CommandsKey.SOURCE_TABLE.getKey(), String.class);
//        commands.setSourceTable(sourceTable);
//
//        return commands;
//    }
}
