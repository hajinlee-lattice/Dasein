package com.latticeengines.dellebi.util;

import java.util.List;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.service.SqoopSyncJobService;
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

    public boolean export(DataFlowContext context) {

        String targetTable = targetRawTable;
        String sourceDir = dellEbiFlowService.getOutputDir(null);
        String successFile = dellEbiFlowService.getOutputDir(null) + "/_SUCCESS";
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

        String targetJdbcDb = dellEbiFlowService.getTargetDB(context);
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(targetJdbcHost).port(Integer.parseInt(targetJdbcPort)).db(targetJdbcDb).user(targetJdbcUser)
                .password(targetJdbcPassword).dbType(targetJdbcType);
        DbCreds creds = new DbCreds(builder);
        String errorMsg = null;
        String queue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
        try {
            sqoopSyncJobService.exportDataSync(targetTable, sourceDir, creds, queue, customer, 4, columns);

        } catch (Exception e) {
            errorMsg = "Export files " + sourceDir + " to SQL server failed! errorMsg=" + e.getMessage();
            log.error("Export files " + sourceDir + " to SQL server failed", e);
        }
        log.info("Finish export HDFS files to SQL server");

        if (errorMsg == null) {
            try {
                if (dellEbiFlowService.runStoredProcedure(context)) {
                    log.info("Begin to execute the Store Procedure= " + quote_sp);
                    sqoopSyncJobService.eval(sqlStr, queue, "Exceute SP-" + quote_sp, creds);
                    log.info("Finished executing the Store Procedure= " + quote_sp);
                }
            } catch (Exception e) {
                errorMsg = "Failed to execute the Store Procedure= " + quote_sp;
                log.error(errorMsg, e);
            }
        }

        String fileName = context.getProperty(DellEbiFlowService.ZIP_FILE_NAME, String.class);
        ;
        if (errorMsg == null) {
            try {
                List<String> files = HdfsUtils.getFilesByGlob(conf, dellEbiFlowService.getTxtDir(null) + "/*.txt");
                if (files != null && files.size() > 0) {
                    boolean result = dellEbiFlowService.deleteFile(context);
                    if (result) {
                        report(context, "Dell EBI daily refresh (export) succeeded!", fileName, targetJdbcDb);
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
            dellEbiFlowService.registerFailedFile(context);
        }
        return false;

    }

    private void report(DataFlowContext requestContext, String msg, String fileName, String targetJdbcDb) {
        String totalTime = getTotalTime(requestContext);
        mailSender.sendEmail(mailReceiveList, msg + " File=" + fileName, "\nEnv = " + dellebiEnv + "\nTotalTime = "
                + totalTime + "\nDB = " + targetJdbcDb);
    }

    private String getTotalTime(DataFlowContext requestContext) {
        Long startTime = requestContext.getProperty(DellEbiFlowService.START_TIME, Long.class);
        if (startTime == null) {
            return "unknow";
        }
        long endTime = System.currentTimeMillis();
        return DurationFormatUtils.formatDuration(endTime - startTime, "HH:mm:ss:SS");
    }
}
