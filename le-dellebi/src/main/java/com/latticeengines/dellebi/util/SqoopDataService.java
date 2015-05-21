package com.latticeengines.dellebi.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.common.exposed.util.HdfsUtils;

public class SqoopDataService {

    private static final Log log = LogFactory.getLog(SqoopDataService.class);

    private DellEBI_SqoopSyncJobService sqoopSyncJobService;

    @Value("${dellebi.datahadooprootpath}")
    private String dataHadoopRootPath;

    @Value("${dellebi.datahadoopworkingpath}")
    private String dataHadoopWorkingPath;

    @Value("${dellebi.ordersummary}")
    private String orderSummary;

    @Value("${dellebi.quotetrans}")
    private String quotetrans;

    @Value("${dellebi.output.table.sample}")
    private String targetRawTable;

    @Value("${dellebi.sqlserverconnection}")
    private String uri;

    @Value("${dellebi.customer}")
    private String customer;

    @Value("${dellebi.quotetrans.storeprocedure}")
    private String quote_sp;

    @Value("${dellebi.output.hdfsdata.remove}")
    private boolean doRemove;

    @Autowired
    private HadoopFileSystemOperations hadoopfilesystemoperations;

    public void export() {

        String targetTable = targetRawTable;
        String queue = null;
        int rc = 1;
        String sourceDir = dataHadoopRootPath + dataHadoopWorkingPath + "/"
                + quotetrans;
        String successFile = sourceDir + "/_SUCCESS";
        String columns = "QuoteNumber,Date,CustomerID,Product,RepBadge,Quantity,Amount,QuoteFileName";
        String sqlStr = "exec " + quote_sp;

        // Pre-check before the exporting operation
        try {
            if (!HdfsUtils.fileExists(new Configuration(), successFile)) {
                log.info("The successFile: " + successFile
                        + " does not exist, skip the data export");
                return;
            }
        } catch (Exception e1) {
            e1.printStackTrace();
            return;
        }

        log.info("Start export HDFS files " + sourceDir);

        sqoopSyncJobService = new DellEBI_SqoopSyncJobService();

        log.info("SQL Server URI: " + uri);

        try {
            rc = sqoopSyncJobService.exportData(targetTable, sourceDir, uri,
                    queue, customer, 4, columns);
        } catch (Exception e) {
            log.error("Exception: Export files " + sourceDir
                    + " to SQL server failed");
        }

        if (rc == 0) {
//            log.info("Begin to execute the Store Procedure: " + quote_sp);
//            rc = sqoopSyncJobService.eval(sqlStr, 1, uri);
        }

        if (rc == 0) {
            if (doRemove) {
                log.info("Remove the exported HDFS file " + sourceDir);
                hadoopfilesystemoperations.cleanFolder(sourceDir);
            } else {
                log.info("Didn't remove the exported HDFS file " + sourceDir);
            }
        } else {
            log.error("Export files " + sourceDir + " to SQL server failed");
        }

        log.info("Finish export HDFS files to SQL server");

        return;

    }

}
