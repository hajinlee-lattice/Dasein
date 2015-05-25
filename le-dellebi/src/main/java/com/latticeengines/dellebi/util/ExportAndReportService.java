package com.latticeengines.dellebi.util;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.service.SqoopSyncJobService;
import com.latticeengines.dellebi.dataprocess.DellEbiDailyJob;
import com.latticeengines.dellebi.service.DellEbiFlowService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.scheduler.exposed.fairscheduler.LedpQueueAssigner;

public class ExportAndReportService {

    private static final Log log = LogFactory.getLog(ExportAndReportService.class);

    @Value("${dellebi.quotetrans}")
    private String quotetrans;

    @Value("${dellebi.output.table.sample}")
    private String targetRawTable;

    @Value("${dellebi.customer}")
    private String customer;

    @Value("${dellebi.quotetrans.storeprocedure}")
    private String quote_sp;

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
    @Value("${dellebi.datatarget.dbname}")
    private String targetJdbcDb;
    @Value("${dellebi.datatarget.type}")
    private String targetJdbcType;
    @Value("${dellebi.datatarget.user}")
    private String targetJdbcUser;
    @Value("${dellebi.datatarget.password.encrypted}")
    private String targetJdbcPassword;

    @Autowired
    private MailSender mailSender;

    @Autowired
    private DellEbiFlowService dellEbiFlowService;

    @Autowired
    private SqoopSyncJobService sqoopSyncJobService;

    @Autowired
    private HadoopFileSystemOperations hadoopfilesystemoperations;

    public boolean export(DataFlowContext requestContext) {

        String targetTable = targetRawTable;
        String sourceDir = dellEbiFlowService.getOutputDir();
        String successFile = dellEbiFlowService.getOutputDir() + "/_SUCCESS";
        String columns = "QuoteNumber,Date,CustomerID,Product,RepBadge,Quantity,Amount,QuoteFileName";
        String sqlStr = "exec " + quote_sp;

        Configuration conf = new Configuration();
        try {
            if (!HdfsUtils.fileExists(conf, successFile)) {
                log.info("The successFile: " + successFile + " does not exist in output, skip the data export");
                return false;
            }
        } catch (Exception ex) {
            log.error("The successFile: " + successFile + " does not exist in output! errorMsg=" + ex.toString());
            return false;
        }

        log.info("Start export from HDFS files " + sourceDir);

        String errorMsg = null;
        try {
            String queue = LedpQueueAssigner.getMRQueueNameForSubmission();
            DbCreds.Builder builder = new DbCreds.Builder();
            builder.host(targetJdbcHost).port(Integer.parseInt(targetJdbcPort)).db(targetJdbcDb).user(targetJdbcUser)
                    .password(targetJdbcPassword).dbType(targetJdbcType);
            DbCreds creds = new DbCreds(builder);
            sqoopSyncJobService.exportData(targetTable, sourceDir, creds, queue, customer, 4, null, columns);

        } catch (Exception e) {
            errorMsg = "Export files " + sourceDir + " to SQL server failed! errorMsg=" + e.getMessage();
            log.error("Export files " + sourceDir + " to SQL server failed", e);
        }

        if (errorMsg == null) {
            // log.info("Begin to execute the Store Procedure: " + quote_sp);
            // rc = sqoopSyncJobService.eval(sqlStr, 1, uri);

            log.info("Finish export HDFS files to SQL server");
        } else {
            errorMsg = "Export files " + sourceDir + " to SQL server failed!";
        }

        String fileName = "";
        if (errorMsg == null) {
            try {
                List<String> files = HdfsUtils.getFilesByGlob(conf, dellEbiFlowService.getTxtDir() + "/*.txt");
                if (files != null && files.size() > 0) {
                    fileName = files.get(0);
                    int idx = fileName.lastIndexOf("/");
                    if (idx >= 0) {
                        fileName = fileName.substring(idx + 1);
                    }
                    fileName = StringUtils.removeEndIgnoreCase(fileName, ".txt") + ".zip";
                    boolean result = dellEbiFlowService.deleteSMBFile(fileName);
                    if (result) {
                        report(requestContext, "Dell EBI daily refresh (export) succeeded!", fileName);
                        return true;
                    } else {
                        errorMsg = "Can not delete smbFile=" + fileName;
                    }
                }

            } catch (Exception ex) {
                errorMsg = "Failed to get export file! errorMsg=" + ex.getMessage();
                log.error("Failed to get export file!", ex);
            }
        }

        if (errorMsg != null) {
            report(requestContext, "Dell EBI daily refresh (export) failed! errorMsg=" + errorMsg, fileName);
            dellEbiFlowService.registerFailedFile(fileName);
        }
        return false;

    }

    private void report(DataFlowContext requestContext, String msg, String fileName) {
        String totalTime = getTotalTime(requestContext);
        mailSender.sendEmail(mailReceiveList, msg + " File=" + fileName, "\nEnv = " + dellebiEnv + "\nTotalTime = "
                + totalTime);
    }

    private String getTotalTime(DataFlowContext requestContext) {
        Long startTime = requestContext.getProperty(DellEbiDailyJob.START_TIME, Long.class);
        if (startTime == null) {
            return "unknow";
        }
        long endTime = System.currentTimeMillis();
        return DurationFormatUtils.formatDuration(endTime - startTime, "HH:mm:ss:SS");
    }
}
