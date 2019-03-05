package com.latticeengines.dellebi.util;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.yarn.client.YarnClient;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.dellebi.entitymanager.DellEbiExecutionLogEntityMgr;
import com.latticeengines.dellebi.service.DellEbiFlowService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.dellebi.DellEbiExecutionLog;
import com.latticeengines.domain.exposed.dellebi.DellEbiExecutionLogStatus;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.proxy.exposed.sqoop.SqoopProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.yarn.exposed.service.JobService;

public class ExportAndReportService {

    private static final Logger log = LoggerFactory.getLogger(ExportAndReportService.class);

    @Value("${dellebi.quotetrans}")
    private String quotetrans;

    @Value("${dellebi.output.table.sample}")
    private String targetRawTable;

    @Value("${dellebi.customer}")
    private String customer;

    @Value("${dellebi.sqoopexporter.mapper.number}")
    private int numMappers;

    @Value("${dellebi.output.hdfsdata.remove}")
    private boolean doRemove;

    @Value("${dellebi.mailreceivelist}")
    private String mailReceiveList;

    @Value("${dellebi.env}")
    private String dellebiEnv;

    @Value("${dellebi.datatarget.host}")
    private String targetJdbcHost;

    @Value("${dellebi.datatarget.port}")
    private String targetJdbcPort;

    @Value("${dellebi.datatarget.type}")
    private String targetJdbcType;

    @Value("${dellebi.datatarget.user}")
    private String targetJdbcUser;

    @Value("${dellebi.datatarget.password.encrypted}")
    private String targetJdbcPassword;

    @Inject
    private MailSender mailSender;

    @Inject
    private DellEbiFlowService dellEbiFlowService;

    @Inject
    private JobService jobService;

    @Inject
    private SqoopProxy sqoopProxy;

    @Inject
    protected Configuration yarnConfiguration;

    @Inject
    protected YarnClient yarnClient;

    @Inject
    private DellEbiExecutionLogEntityMgr dellEbiExecutionLogEntityMgr;

    public boolean export(DataFlowContext context) {

        String sourceDir = dellEbiFlowService.getOutputDir(context);
        String successFile = dellEbiFlowService.getOutputDir(context) + "/_SUCCESS";
        DellEbiExecutionLog dellEbiExecutionLog = context.getProperty(DellEbiFlowService.LOG_ENTRY,
                DellEbiExecutionLog.class);
        Long startTime = System.currentTimeMillis();

        try {
            if (!HdfsUtils.fileExists(yarnConfiguration, successFile)) {
                log.info("The successFile: " + successFile + " does not exist in output, skip the data export");
                return false;
            }
        } catch (Exception ex) {
            log.error("The successFile: " + successFile + " does not exist in output! errorMsg=" + ex.toString());
            return false;
        }

        log.info("Start export from HDFS files " + sourceDir);

        String targetTable = dellEbiFlowService.getTargetTable(context);

        String targetJdbcDb = dellEbiFlowService.getTargetDB(context);
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(targetJdbcHost).port(Integer.parseInt(targetJdbcPort)).db(targetJdbcDb).user(targetJdbcUser)
                .clearTextPassword(targetJdbcPassword).dbType(targetJdbcType);
        DbCreds creds = new DbCreds(builder);
        String errorMsg = null;
        String queue = LedpQueueAssigner.getPropDataQueueNameForSubmission();

        List<String> targetColumns = Arrays.asList(dellEbiFlowService.getTargetColumns(context));
        String optionalEnclosurePara = "--fields-terminated-by";
        String optionalEnclosureValue = "\t";

        SqoopExporter exporter = new SqoopExporter.Builder().setTable(targetTable).setDbCreds(creds)
                .setSourceDir(sourceDir).setQueue(queue).setCustomer(customer).setNumMappers(numMappers).setSync(false)
                .setExportColumns(targetColumns).addExtraOption(optionalEnclosurePara)
                .addExtraOption(optionalEnclosureValue).build();

        try {
            ApplicationId appId = ConverterUtils
                    .toApplicationId(sqoopProxy.exportData(exporter).getApplicationIds().get(0));
            FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnClient, appId, 3600);

            if (!FinalApplicationStatus.SUCCEEDED.equals(status)) {
                boolean isRunning = isJobRunning(appId);
                jobService.killJob(appId);
                if (isRunning) {
                    throw new IllegalStateException(appId + " is running but stuck, the job is killed now");
                }
                throw new IllegalStateException("The final state of " + appId + " is not "
                        + FinalApplicationStatus.SUCCEEDED + " but rather " + status);
            }

        } catch (Exception e) {
            errorMsg = "Export files " + sourceDir + " to SQL server failed! errorMsg=" + e.getMessage();
            log.error("Export files " + sourceDir + " to SQL server failed", e);
        }

        if (errorMsg == null) {

            dellEbiExecutionLog.setStatus(DellEbiExecutionLogStatus.Exported.getStatus());
            dellEbiExecutionLogEntityMgr.executeUpdate(dellEbiExecutionLog);

            LoggingUtils.logInfoWithDuration(log, dellEbiExecutionLog, "Finish exporting HDFS files to SQL server",
                    startTime);

            try {
                startTime = System.currentTimeMillis();
                dellEbiFlowService.runStoredProcedure(context);
                LoggingUtils.logInfoWithDuration(log, dellEbiExecutionLog, "Finish executing the Store Procedure",
                        startTime);
            } catch (Exception e) {
                errorMsg = "Failed to execute the Store Procedure";
                log.error(errorMsg, e);
            }
        }

        String fileName = context.getProperty(DellEbiFlowService.ZIP_FILE_NAME, String.class);

        if (errorMsg == null) {
            try {
                List<String> files = HdfsUtils.getFilesByGlob(yarnConfiguration,
                        dellEbiFlowService.getTxtDir(context) + "/*.txt");
                if (files != null && files.size() > 0) {
                    boolean result = dellEbiFlowService.deleteFile(context);
                    if (result) {
                        report(context, "Dell EBI refresh successfully!", fileName, targetJdbcDb);
                        dellEbiExecutionLog.setStatus(DellEbiExecutionLogStatus.Completed.getStatus());
                        dellEbiExecutionLog.setEndDate(new Date());
                        dellEbiExecutionLogEntityMgr.executeUpdate(dellEbiExecutionLog);
                        LoggingUtils.logInfoWithDuration(log, dellEbiExecutionLog, "Dell EBI refresh successfully!",
                                dellEbiExecutionLog.getStartDate().getTime());
                        return true;
                    } else {
                        errorMsg = "Can not delete smbFile=" + fileName;
                    }
                } else {
                    errorMsg = "Can not find txt file for " + fileName;
                }

            } catch (Exception ex) {
                errorMsg = "Failed to get export file! errorMsg=" + ex.getMessage();
                log.error("Failed to get export file!", ex);
            }
        }

        if (errorMsg != null) {
            report(context, "Dell EBI daily refresh (export) failed! errorMsg=" + errorMsg, fileName, targetJdbcDb);
            dellEbiFlowService.registerFailedFile(context, errorMsg);
        }
        return false;
    }

    private void report(DataFlowContext requestContext, String msg, String fileName, String targetJdbcDb) {
        String totalTime = getTotalTime(requestContext);
        mailSender.sendEmail(mailReceiveList, msg + " File=" + fileName,
                "\nEnv = " + dellebiEnv + "\nTotalTime = " + totalTime + "\nDB = " + targetJdbcDb);
    }

    private String getTotalTime(DataFlowContext requestContext) {
        Long startTime = requestContext.getProperty(DellEbiFlowService.START_TIME, Long.class);
        if (startTime == null) {
            return "unknow";
        }
        long endTime = System.currentTimeMillis();
        return DurationFormatUtils.formatDuration(endTime - startTime, "HH:mm:ss:SS");
    }

    private Boolean isJobRunning(ApplicationId applicationId) {
        try {
            ApplicationReport report = YarnUtils.getApplicationReport(yarnClient, applicationId);
            return (report.getYarnApplicationState().equals(YarnApplicationState.RUNNING));
        } catch (Exception e) {
            log.warn("Failed to get application status of application id " + applicationId);
        }
        return false;
    }
}
