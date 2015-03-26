package com.latticeengines.madison.service.impl;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.MadisonLogicDailyProgress;
import com.latticeengines.domain.exposed.propdata.MadisonLogicDailyProgressStatus;
import com.latticeengines.jobs.PropDataJobService;
import com.latticeengines.madison.entitymanager.PropDataMadisonEntityMgr;
import com.latticeengines.madison.service.PropDataMadisonDataFlowService;
import com.latticeengines.madison.service.PropDataMadisonService;
import com.latticeengines.propdata.service.db.PropDataContext;
import com.latticeengines.scheduler.exposed.fairscheduler.LedpQueueAssigner;

@Component("propDataMadisonService")
public class PropDataMadisonServiceImpl implements PropDataMadisonService {

    private static final String DATE_FORMAT = "yyyy-MM-dd";

    private final Log log = LogFactory.getLog(this.getClass());

    @Autowired
    private PropDataMadisonEntityMgr propDataMadisonEntityMgr;

    @Autowired
    private PropDataJobService propDataJobService;

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

            String assignedQueue = LedpQueueAssigner.getMRQueueNameForSubmission();
            propDataJobService.importData(dailyProgress.getDestinationTable(), targetDir, assignedQueue, getJobName()
                    + "-Progress Id-" + dailyProgress.getPid(), splitColumns, numMappers,
                    getConnectionString(sourceDataJdbcUrl, sourceDataJdbcUser, sourceDataJdbcPassword));

            dailyProgress.setStatus(MadisonLogicDailyProgressStatus.FINISHED.getStatus());
            propDataMadisonEntityMgr.executeUpdate(dailyProgress);

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
                    return response;
                } else {
                    log.warn("Cleanup dir=" + targetDir);
                    HdfsUtils.rmdir(yarnConfiguration, targetDir);
                }
            }

            List<String> pastDaysPaths = new ArrayList<>();
            List<Date> pastDays = new ArrayList<>();
            getPastIncrementalPaths(today, pastDays, pastDaysPaths);
            if (pastDaysPaths.size() == 0) {
                log.warn("There's not incremental data found.");
                return response;
            }
            log.info("The batch size of incremental data = " + pastDaysPaths.size());
            log.info("The incremental timestamps of incremental data = " + pastDaysPaths);

            transformData(today, pastDays);
            response.setProperty(TODAY_KEY, today);
            response.setProperty(STATUS_KEY, STATUS_OK);

        } catch (Exception ex) {
            log.info("Transform failed!", ex);
            throw new LedpException(LedpCode.LEDP_00002, ex);
        }
        return response;
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
                String assignedQueue = LedpQueueAssigner.getMRQueueNameForSubmission();
                propDataJobService.importData(targetTable + "_new", schemaPath, assignedQueue,
                        getJobName() + "-schema", "DomainID", 1,
                        getConnectionString(targetJdbcUrl, targetJdbcUser, targetJdbcPassword));
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

    private void getPastIncrementalPaths(Date today, List<Date> pastDays, List<String> pastDaysPaths) throws Exception {
        try {
            String todayIncrementalPath = getHdfsDataflowIncrementalRawPathWithDate(today);
            if (!HdfsUtils.fileExists(yarnConfiguration, todayIncrementalPath)) {
                log.info("There's no incremental data for today.");
                return;
            }

            String path = todayIncrementalPath;
            Date date = today;
            pastDays.add(date);
            pastDaysPaths.add(path);
            for (int i = 0; i < numOfPastDays - 1; i++) {
                date = DateUtils.addDays(date, -1);
                path = getHdfsDataflowIncrementalRawPathWithDate(date);
                if (HdfsUtils.fileExists(yarnConfiguration, getSuccessFile(path))) {
                    pastDays.add(date);
                    pastDaysPaths.add(path);
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
            log.info("Uploading today's aggregation data=" + sourceDir);
            String assignedQueue = LedpQueueAssigner.getMRQueueNameForSubmission();
            propDataJobService.exportData(getTableNew(), getOutputDir(sourceDir), assignedQueue, getJobName()
                    + "-uploadAggregationData", numMappers,
                    getConnectionString(targetJdbcUrl, targetJdbcUser, targetJdbcPassword));

            swapTargetTables(assignedQueue);
            uploadTodayRawData(today);

            response.setProperty(TODAY_KEY, today);
            response.setProperty(STATUS_KEY, STATUS_OK);

        } catch (Exception ex) {
            log.info("exportToDB failed!", ex);
            throw new LedpException(LedpCode.LEDP_00002, ex);
        }
        return response;
    }

    private String getOutputDir(String sourceDir) {
        return sourceDir + "/output";
    }

    private String getTableNew() {
        return targetTable + "_new";
    }

    private void uploadTodayRawData(Date today) throws Exception {
        String todayIncrementalPath = getHdfsDataflowIncrementalRawPathWithDate(today);
        if (!HdfsUtils.fileExists(yarnConfiguration, todayIncrementalPath)) {
            log.error("There's no incremental data for today.");
            return;
        }
        log.info("Uploading today's raw data=" + todayIncrementalPath);
        String assignedQueue = LedpQueueAssigner.getMRQueueNameForSubmission();
        propDataJobService.exportData(targetRawTable, todayIncrementalPath, assignedQueue, getJobName()
                + "-uploadRawData", numMappers, getConnectionString(targetJdbcUrl, targetJdbcUser, targetJdbcPassword));
        log.info("Finished uploading today's raw data=" + todayIncrementalPath);

    }

    void cleanupTargetRawData() throws Exception {
        String assignedQueue = LedpQueueAssigner.getMRQueueNameForSubmission();
        propDataJobService.eval("TRUNCATE TABLE " + targetRawTable, assignedQueue, getJobName() + "-cleanRawTable", 1,
                getConnectionString(targetJdbcUrl, targetJdbcUser, targetJdbcPassword));
    }

    void swapTargetTables(String assignedQueue) {
        List<String> sqls = buildSqls(targetTable);
        propDataJobService.eval(StringUtils.join(sqls, ";"), assignedQueue, getJobName() + "-swapTables", 1,
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
