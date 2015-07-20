package com.latticeengines.propdata.madison.service.impl;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.service.SqoopSyncJobService;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.propdata.MadisonLogicDailyProgress;
import com.latticeengines.domain.exposed.propdata.MadisonLogicDailyProgressStatus;
import com.latticeengines.propdata.eai.service.PropDataContext;
import com.latticeengines.propdata.madison.entitymanager.PropDataMadisonEntityMgr;
import com.latticeengines.propdata.madison.service.PropDataMadisonDataFlowService;
import com.latticeengines.propdata.madison.service.PropDataMadisonService;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component("propDataMadisonService")
public class PropDataMadisonServiceImpl implements PropDataMadisonService {

    private static final String DATE_FORMAT = "yyyy-MM-dd";

    private final Log log = LogFactory.getLog(this.getClass());

    @Autowired
    private PropDataMadisonEntityMgr propDataMadisonEntityMgr;

    @Autowired
    private SqoopSyncJobService propDataJobService;

    @Autowired
    private PropDataMadisonDataFlowService propDataMadisonDataFlowService;

    @Autowired
    protected Configuration yarnConfiguration;

    @Value("${propdata.madison.datasource.url}")
    private String sourceJdbcUrl;
    @Value("${propdata.madison.datasource.user}")
    private String sourceJdbcUser;
    @Value("${propdata.madison.datasource.password.encrypted}")
    private String sourceJdbcPassword;

    @Value("${propdata.madison.datasource.data.url}")
    private String sourceDataJdbcUrl;
    @Value("${propdata.madison.datasource.data.host}")
    private String sourceDataJdbcHost;
    @Value("${propdata.madison.datasource.data.port}")
    private String sourceDataJdbcPort;
    @Value("${propdata.madison.datasource.data.dbname}")
    private String sourceDataJdbcDb;
    @Value("${propdata.madison.datasource.data.type}")
    private String sourceDataJdbcType;
    @Value("${propdata.madison.datasource.data.user}")
    private String sourceDataJdbcUser;
    @Value("${propdata.madison.datasource.data.password.encrypted}")
    private String sourceDataJdbcPassword;

    @Value("${propdata.basedir}")
    private String propdataBaseDir;
    @Value("${propdata.data.source.dir}")
    private String propdataSourceDir;

    @Value("${propdata.madison.mapper.number}")
    private int numMappers;
    @Value("${propdata.madison.split.columns}")
    private String splitColumns;

    @Value("${propdata.madison.num.past.days}")
    private int numOfPastDays;

    @Value("${propdata.madison.target.raw.table}")
    private String targetRawTable;
    @Value("${propdata.madison.target.table}")
    private String targetTable;
    @Value("${propdata.madison.datatarget.url}")
    private String targetJdbcUrl;
    @Value("${propdata.madison.datatarget.host}")
    private String targetJdbcHost;
    @Value("${propdata.madison.datatarget.port}")
    private String targetJdbcPort;
    @Value("${propdata.madison.datatarget.dbname}")
    private String targetJdbcDb;
    @Value("${propdata.madison.datatarget.type}")
    private String targetJdbcType;
    @Value("${propdata.madison.datatarget.user}")
    private String targetJdbcUser;
    @Value("${propdata.madison.datatarget.password.encrypted}")
    private String targetJdbcPassword;

    @Override
    public PropDataContext importFromDB(PropDataContext requestContext) {

        PropDataContext response = new PropDataContext();
        MadisonLogicDailyProgress dailyProgress = requestContext.getProperty(RECORD_KEY,
                MadisonLogicDailyProgress.class);
        if (dailyProgress == null) {
            dailyProgress = propDataMadisonEntityMgr.getNextAvailableDailyProgress();
        }
        if (dailyProgress == null) {
            log.info("there's no record in daily progress table.");
            return response;
        }
        log.info("Processing daily progress record=" + dailyProgress.toString());

        try {
            String targetDir = getHdfsDataflowIncrementalRawPathWithDate(dailyProgress.getFileDate());
            if (HdfsUtils.fileExists(yarnConfiguration, targetDir)) {
                if (HdfsUtils.fileExists(yarnConfiguration, getSuccessFile(targetDir))) {
                    dailyProgress.setStatus(MadisonLogicDailyProgressStatus.FINISHED.getStatus());
                    propDataMadisonEntityMgr.executeUpdate(dailyProgress);
                    log.info("Data is already processed for record=" + dailyProgress);
                    return response;

                } else {
                    log.warn("Cleanup dir=" + targetDir);
                    HdfsUtils.rmdir(yarnConfiguration, targetDir);
                }
            }

            String assignedQueue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
            DbCreds.Builder builder = new DbCreds.Builder();
            builder.host(sourceDataJdbcHost).port(Integer.parseInt(sourceDataJdbcPort)).db(sourceDataJdbcDb)
                    .user(sourceDataJdbcUser).password(sourceDataJdbcPassword).dbType(sourceDataJdbcType);
            DbCreds creds = new DbCreds(builder);
            propDataJobService.importDataSync(dailyProgress.getDestinationTable(), targetDir, creds, assignedQueue,
                    getJobName() + "-Progress Id-" + dailyProgress.getPid(), Arrays.asList(splitColumns.split(",")),
                    "", numMappers);

            dailyProgress.setStatus(MadisonLogicDailyProgressStatus.FINISHED.getStatus());
            propDataMadisonEntityMgr.executeUpdate(dailyProgress);
            HdfsUtils.writeToFile(yarnConfiguration, getTableNameFromFile(targetDir),
                    dailyProgress.getDestinationTable());

            response.setProperty(RESULT_KEY, dailyProgress);
            response.setProperty(STATUS_KEY, STATUS_OK);

            log.info("Finished job id=" + dailyProgress.getPid());

        } catch (Exception ex) {
            setFailed(dailyProgress, ex);
            log.info("Import failed!", ex);
            throw new LedpException(LedpCode.LEDP_00002, ex);
        }

        return response;
    }

    private String getTableNameFromFile(String targetDir) {
        return targetDir + "/_TABLENAME";
    }

    @Override
    public PropDataContext transform(PropDataContext requestContext) {
        PropDataContext response = new PropDataContext();
        Date today = requestContext.getProperty(TODAY_KEY, Date.class);
        if (today == null) {
            today = new Date();
        }
        String targetDir = getHdfsWorkflowTotalRawPath(today);
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, targetDir)) {
                if (HdfsUtils.fileExists(yarnConfiguration, getSuccessFile(getOutputDir(targetDir)))) {
                    log.info("Data is already transformed for today=" + today.toString());
                    response.setProperty(TODAY_KEY, today);
                    response.setProperty(STATUS_KEY, STATUS_OK);
                    return response;
                } else {
                    log.warn("Cleanup dir=" + targetDir);
                    HdfsUtils.rmdir(yarnConfiguration, targetDir);
                }
            }

            List<Date> pastDays = new ArrayList<>();
            getPastIncrementalDays(today, pastDays);
            if (pastDays.size() == 0) {
                log.warn("There's no incremental data found for today.");
                today = findPreviousAvailableDays(today, pastDays);
                if (pastDays.size() == 0) {
                    return response;
                }
            }
            log.info("The batch size of incremental data = " + pastDays.size());
            log.info("The incremental timestamps of incremental data = " + pastDays);

            transformData(today, pastDays);
            response.setProperty(TODAY_KEY, today);
            response.setProperty(STATUS_KEY, STATUS_OK);

        } catch (Exception ex) {
            log.info("Transform failed!", ex);
            throw new LedpException(LedpCode.LEDP_00002, ex);
        }
        return response;
    }

    private Date findPreviousAvailableDays(Date today, List<Date> pastDays) throws Exception {
        Date newDay = today;
        for (int i = 0; i < 5; i++) {
            newDay = DateUtils.addDays(newDay, -1);
            String targetDir = getHdfsWorkflowTotalRawPath(newDay);
            if (!HdfsUtils.fileExists(yarnConfiguration, getSuccessFile(getOutputDir(targetDir)))) {
                getPastIncrementalDays(newDay, pastDays);
                if (pastDays.size() > 0) {
                    return newDay;
                }
            }
        }
        return today;
    }

    private void transformData(Date today, List<Date> pastDays) throws Exception {
        String sourcePathRegEx = getSourcePathRegEx(pastDays);
        List<String> sourcePaths = new ArrayList<>();
        sourcePaths.add(getHdfsDataflowIncrementalRawPathWithName(sourcePathRegEx));

        Date pastday = DateUtils.addDays(today, -1 * numOfPastDays);
        String pastDayAggregation = getHdfsWorkflowTotalRawPath(pastday);
        if (pastday.before(today)
                && HdfsUtils.fileExists(yarnConfiguration, getSuccessFile(getOutputDir(pastDayAggregation)))) {
            sourcePaths.add(getHdfsWorkflowTotalRawPath(pastday));
        }

        String targetSchemaPath = getHdfsWorkflowTargetSchemaPath();
        propDataMadisonDataFlowService.execute(getJobName(), sourcePaths, getHdfsWorkflowTotalRawPath(today),
                targetSchemaPath);

    }

    private String getHdfsWorkflowTargetSchemaPath() {
        String schemaPath = propdataBaseDir + "/" + propdataSourceDir + "/workflow/" + getJobName() + "/schema";
        try {
            if (!HdfsUtils.fileExists(yarnConfiguration, getSuccessFile(schemaPath))) {
                if (HdfsUtils.fileExists(yarnConfiguration, schemaPath)) {
                    HdfsUtils.rmdir(yarnConfiguration, schemaPath);
                }
                String assignedQueue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
                DbCreds.Builder builder = new DbCreds.Builder();
                builder.host(targetJdbcHost).port(Integer.parseInt(targetJdbcPort)).db(targetJdbcDb)
                        .user(targetJdbcUser).password(targetJdbcPassword).dbType(targetJdbcType);
                DbCreds creds = new DbCreds(builder);
                propDataJobService.importDataSync(targetTable + "_new", schemaPath, creds, assignedQueue, getJobName()
                        + "-schema", Arrays.asList("DomainID"), "", 1);
                log.info("Finished getting targetTable's schema file=" + schemaPath);
            }
        } catch (Exception e) {
            throw new RuntimeException("Can not get tareget table's schema file");
        }

        return schemaPath;
    }

    private String getSourcePathRegEx(List<Date> pastDays) {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        for (Date date : pastDays) {
            String formatted = getDateStringFormat(date);
            builder.append(formatted).append(",");
        }
        builder.setLength(builder.length() - 1);
        builder.append("}");
        return builder.toString();

    }

    private void getPastIncrementalDays(Date today, List<Date> pastDays) throws Exception {
        try {
            String todayIncrementalPath = getHdfsDataflowIncrementalRawPathWithDate(today);
            if (!HdfsUtils.fileExists(yarnConfiguration, todayIncrementalPath)) {
                log.info("There's no incremental data for date=" + today);
                return;
            }

            String path = todayIncrementalPath;
            Date date = today;
            pastDays.add(date);
            for (int i = 0; i < numOfPastDays - 1; i++) {
                date = DateUtils.addDays(date, -1);
                path = getHdfsDataflowIncrementalRawPathWithDate(date);
                if (HdfsUtils.fileExists(yarnConfiguration, getSuccessFile(path))) {
                    pastDays.add(date);
                }
            }

        } catch (Exception ex) {
            log.error("Failed to get HDFS paths", ex);
            throw ex;
        }
    }

    @Override
    public PropDataContext exportToDB(PropDataContext requestContext) {

        PropDataContext response = new PropDataContext();
        Date today = requestContext.getProperty(TODAY_KEY, Date.class);
        if (today == null) {
            log.warn("There's no aggregated data for today.");
            return response;
        }

        String sourceDir = getHdfsWorkflowTotalRawPath(today);
        try {
            if (!HdfsUtils.fileExists(yarnConfiguration, getSuccessFile(getOutputDir(sourceDir)))) {
                log.warn("There's no aggregated data for today.");
                return response;
            }
            if (HdfsUtils.fileExists(yarnConfiguration, getExportSuccessFile(getOutputDir(sourceDir)))) {
                log.warn("Data has already be exported for today.");
                return response;
            }

            uploadTodayRawData(today);

            uploadAggregateData(sourceDir);

            HdfsUtils.writeToFile(yarnConfiguration, getExportSuccessFile(getOutputDir(sourceDir)), "EXPORT_SUCCESS");

            response.setProperty(TODAY_KEY, today);
            response.setProperty(STATUS_KEY, STATUS_OK);

        } catch (Exception ex) {
            log.info("exportToDB failed!", ex);
            throw new LedpException(LedpCode.LEDP_00002, ex);
        }
        return response;
    }

    private void uploadAggregateData(String sourceDir) {
        log.info("Uploading today's aggregation data=" + sourceDir);
        String assignedQueue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
        truncateNewTable(assignedQueue);

        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(targetJdbcHost).port(Integer.parseInt(targetJdbcPort)).db(targetJdbcDb).user(targetJdbcUser)
                .password(targetJdbcPassword).dbType(targetJdbcType);
        DbCreds creds = new DbCreds(builder);
        propDataJobService.exportDataSync(getTableNew(), getOutputDir(sourceDir), creds, assignedQueue, getJobName()
                + "-uploadAggregationData", numMappers, null);

        swapTargetTables(assignedQueue);
    }

    private void truncateNewTable(String assignedQueue) {
        String sql = "TRUNCATE TABLE " + getTableNew();
        propDataJobService.eval(sql, assignedQueue, getJobName() + "-truncateNewTable",
                getConnectionString(targetJdbcUrl, targetJdbcUser, targetJdbcPassword));
    }

    private String getOutputDir(String sourceDir) {
        return sourceDir + "/output";
    }

    private String getTableNew() {
        return targetTable + "_new";
    }

    private void uploadTodayRawData(Date today) throws Exception {

        String todayIncrementalPath = getHdfsDataflowIncrementalRawPathWithDate(today);
        if (!HdfsUtils.fileExists(yarnConfiguration, getTableNameFromFile(todayIncrementalPath))) {
            log.error("There's no incremental data for today.");
            return;
        }
        if (StringUtils.isEmpty(targetRawTable)) {
            log.info("targetRawTable was not set, it won't be loaded");
            return;
        }

        log.info("Uploading today's raw data=" + todayIncrementalPath);
        String tableName = HdfsUtils.getHdfsFileContents(yarnConfiguration, getTableNameFromFile(todayIncrementalPath));

        String assignedQueue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
        String connectionString = getConnectionString(targetJdbcUrl, targetJdbcUser, targetJdbcPassword);

        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(targetJdbcHost).port(Integer.parseInt(targetJdbcPort)).db(targetJdbcDb).user(targetJdbcUser)
                .password(targetJdbcPassword).dbType(targetJdbcType);
        DbCreds creds = new DbCreds(builder);
        propDataJobService.eval("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'" + tableName
                + "') AND type in (N'U')) DROP TABLE " + tableName, assignedQueue, getJobName() + "-dropRawTable",
                connectionString);
        propDataJobService.eval("SELECT TOP 0 ID AS ID1, * INTO " + tableName + " FROM " + targetRawTable
                + ";ALTER TABLE " + tableName + " DROP COLUMN ID1", assignedQueue, getJobName()
                + "-uploadRawDataCreateTable", connectionString);
        log.info("Uploading today's data, targetTable=" + tableName + " connectionUrl=" + connectionString);

        propDataJobService.exportDataSync(tableName, todayIncrementalPath, creds, assignedQueue, getJobName()
                + "-uploadRawDataExportData", numMappers, null);
        propDataJobService.eval("EXEC MadisonLogic_MergeDailyDepivoted " + tableName, assignedQueue, getJobName()
                + "-uploadRawDataMergeTable", connectionString);
        log.info("Finished uploading today's raw data=" + todayIncrementalPath);

    }

    void cleanupTargetRawData(Date date) throws Exception {
        String assignedQueue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
        String tableName = getTableName(date);

        propDataJobService.eval("DROP TABLE " + tableName, assignedQueue, getJobName() + "-dropRawTable",
                getConnectionString(targetJdbcUrl, targetJdbcUser, targetJdbcPassword));
    }

    String getTableName(Date date) throws Exception {
        String targetDir = getHdfsDataflowIncrementalRawPathWithDate(date);
        String tableName = HdfsUtils.getHdfsFileContents(yarnConfiguration, getTableNameFromFile(targetDir));
        return tableName;
    }

    void swapTargetTables(String assignedQueue) {
        List<String> sqls = buildSqls(targetTable);
        propDataJobService.eval(StringUtils.join(sqls, ";"), assignedQueue, getJobName() + "-swapTables",
                getConnectionString(targetJdbcUrl, targetJdbcUser, targetJdbcPassword));
    }

    private List<String> buildSqls(String targetTable) {
        List<String> sqls = new ArrayList<>();
        sqls.add("EXEC sp_rename " + targetTable + ", " + targetTable + "_bak");
        sqls.add("EXEC sp_rename " + targetTable + "_new, " + targetTable);
        sqls.add("EXEC sp_rename " + targetTable + "_bak, " + targetTable + "_new");
        sqls.add("TRUNCATE TABLE " + targetTable + "_new");
        return sqls;
    }

    private String getJobName() {
        return "MadisonLogic-Days-" + numOfPastDays;
    }

    String getSuccessFile(String targetDir) {
        return targetDir + "/_SUCCESS";
    }

    String getExportSuccessFile(String targetDir) {
        return targetDir + "/_SUCCESS_EXPORT";
    }

    private void setFailed(MadisonLogicDailyProgress dailyProgress, Exception ex) {
        dailyProgress.setStatus(MadisonLogicDailyProgressStatus.FAILED.getStatus());
        dailyProgress.setErrorMessage(ex.getMessage());
        propDataMadisonEntityMgr.executeUpdate(dailyProgress);
    }

    String getHdfsDataflowIncrementalRawPathWithDate(Date fileDate) throws Exception {
        String formatted = getDateStringFormat(fileDate);
        return getHdfsDataflowIncrementalRawPathWithName(formatted);
    }

    private String getHdfsDataflowIncrementalRawPathWithName(String formatted) {
        return propdataBaseDir + "/" + propdataSourceDir + "/dataflow/incremental/" + formatted + "/raw";
    }

    private String getDateStringFormat(Date fileDate) {
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        String formatted = format.format(fileDate);
        return formatted;
    }

    String getHdfsWorkflowTotalRawPath(Date fileDate) {
        String formatted = getDateStringFormat(fileDate);
        return propdataBaseDir + "/" + propdataSourceDir + "/workflow/" + getJobName() + "/total_aggregation/"
                + formatted;
    }

    private String getConnectionString(String jdbcUrl, String jdbcuser, String jdbcPassord) {

        String driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            throw new LedpException(LedpCode.LEDP_11000, e, new String[] { driverClass });
        }
        return jdbcUrl + "user=" + jdbcuser + ";password=" + jdbcPassord;
    }
}
