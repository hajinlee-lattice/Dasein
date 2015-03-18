package com.latticeengines.madison.service.impl;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
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
    private String jdbcUrl;
    @Value("${propdata.madison.datasource.user}")
    private String jdbcUser;
    @Value("${propdata.madison.datasource.password.encrypted}")
    private String jdbcPassword;

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

    @Override
    public PropDataContext importFromDB(PropDataContext requestContext) {
        PropDataContext response = new PropDataContext();
        MadisonLogicDailyProgress dailyProgress = propDataMadisonEntityMgr.getNextAvailableDailyProgress();

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
            ApplicationId applicationId = propDataJobService.importData(dailyProgress.getDestinationTable(), targetDir,
                    assignedQueue, "MadisonLogic-" + dailyProgress.getPid(), splitColumns, numMappers,
                    getConnectionString());

            dailyProgress.setStatus(MadisonLogicDailyProgressStatus.FINISHED.getStatus());
            propDataMadisonEntityMgr.executeUpdate(dailyProgress);

            response.setProperty("applicationId", applicationId);
            response.setProperty("result", dailyProgress);

            log.info("Finished job id=" + dailyProgress.getPid() + " applicaiton Id=" + applicationId);

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
        Date today = requestContext.getProperty("today", Date.class);
        if (today == null) {
            today = new Date();
        }
        String targetDir = getHdfsWorkflowTotalRawPath(today);
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, targetDir)) {
                if (HdfsUtils.fileExists(yarnConfiguration, getSuccessFile(targetDir))) {
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

        } catch (Exception ex) {
            log.info("Transform failed!", ex);
            throw new LedpException(LedpCode.LEDP_00002, ex);
        }
        return response;
    }

    private void transformData(Date today, List<Date> pastDays) throws Exception {
        String sourcePathRegEx = getSourcePathRegEx(pastDays);
        List<String> sourcePaths = new ArrayList<>();
        sourcePaths.add(getHdfsDataflowIncrementalRawPathWithName(sourcePathRegEx) + "/*.avro");

        Date pastday = DateUtils.addDays(today, numOfPastDays);
        String yesterdayAggregation = getHdfsWorkflowTotalRawPath(pastday);
        if (HdfsUtils.fileExists(yarnConfiguration, getSuccessFile(yesterdayAggregation))) {
            sourcePaths.add(getHdfsWorkflowTotalRawPath(pastday) + "/*.avro");
        }
        propDataMadisonDataFlowService.execute("MadisonLogic-" + getDateStringFormat(today), sourcePaths,
                getHdfsWorkflowTotalRawPath(today));

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

    private void getPastIncrementalPaths(Date today, List<Date> pastDays, List<String> pastDaysPaths) {
        try {
            String todayTotalAggregationPath = getHdfsWorkflowTotalRawPath(today);
            if (HdfsUtils.fileExists(yarnConfiguration, getSuccessFile(todayTotalAggregationPath))) {
                log.info("Today's aggregation was already done.");
                return;
            }

            String todayIncrementalPath = getHdfsDataflowIncrementalRawPathWithDate(today);
            if (!HdfsUtils.fileExists(yarnConfiguration, todayIncrementalPath)) {
                log.info("There's no incremental data for today.");
                return;
            }

            String path = todayIncrementalPath;
            Date date = today;
            pastDays.add(today);
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
            log.warn("Failed to get HDFS paths", ex);
        }
    }

    @Override
    public PropDataContext exportToDB(PropDataContext requestContext) {

        return null;
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
        return propdataBaseDir + "/" + propdataSourceDir + "/workflow/total_aggregation/" + formatted;
    }

    private String getConnectionString() {

        String driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            throw new LedpException(LedpCode.LEDP_11000, e, new String[] { driverClass });
        }
        return jdbcUrl + "user=" + jdbcUser + ";password=" + jdbcPassword;
    }
}
