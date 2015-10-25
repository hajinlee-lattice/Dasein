package com.latticeengines.dataplatform.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.sqoop.LedpSqoop;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.yarn.client.YarnClient;

import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.dataplatform.exposed.service.JobNameService;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.dataplatform.exposed.service.SqoopSyncJobService;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.DbCreds;

@Component("sqoopSyncJobService")
public class SqoopSyncJobServiceImpl extends SqoopJobServiceImpl implements SqoopSyncJobService {

    protected static final Log log = LogFactory.getLog(SqoopSyncJobServiceImpl.class);

    @Autowired
    private Configuration hadoopConfiguration;

    @Autowired
    private JobNameService jobNameService;

    @Autowired
    private MetadataService metadataService;

    @Autowired
    protected YarnClient defaultYarnClient;

    private static final int MAX_SQOOP_RETRY = 3;

    @Override
    public ApplicationId importData(String table, String targetDir, DbCreds creds, String queue, String customer,
            List<String> splitCols, String columnsToInclude, int numMappers) {
        return importData(table, targetDir, creds, queue, customer, splitCols, columnsToInclude, numMappers, null);
    }

    @Override
    public ApplicationId importData(String table, String targetDir, DbCreds creds, String queue, String customer,
            List<String> splitCols, String columnsToInclude) {
        int numDefaultMappers = hadoopConfiguration.getInt("mapreduce.map.cpu.vcores", 8);
        return importData(table, targetDir, creds, queue, customer, splitCols, columnsToInclude, numDefaultMappers);
    }

    @Override
    public ApplicationId importDataForQuery(String query, String targetDir, DbCreds creds, String queue, String customer,
            List<String> splitCols, String columnsToInclude) {
        int numDefaultMappers = hadoopConfiguration.getInt("mapreduce.map.cpu.vcores", 8);
        return importDataForQuery(query, targetDir, creds, queue, customer, splitCols, columnsToInclude, numDefaultMappers, null);
    }

    @Override
    public ApplicationId importDataForQuery(String query, String targetDir, DbCreds creds, String queue, String customer,
            List<String> splitCols, String columnsToInclude, int numMappers, Properties props) {
        return importData(null, //
                query, //
                targetDir, //
                creds, //
                queue, //
                customer, //
                splitCols, //
                columnsToInclude, //
                numMappers, //
                props);
    }
    @Override
    public ApplicationId importData(String table, String targetDir, DbCreds creds, String queue, String customer,
            List<String> splitCols, String columnsToInclude, int numMappers, Properties props) {
        return importData(table, //
                           null, //
                           targetDir, //
                           creds, //
                           queue, //
                           customer, //
                           splitCols, //
                           columnsToInclude, //
                           numMappers, //
                           props);
    }
    @Override
    public ApplicationId importData(String table, String query, String targetDir, DbCreds creds, String queue, String customer,
            List<String> splitCols, String columnsToInclude, int numMappers, Properties props) {

        long time1 = System.currentTimeMillis();
        final String jobName = jobNameService.createJobName(customer, "sqoop-import");

        int retryCount = 0;
        while (retryCount < MAX_SQOOP_RETRY) {
            try {
                ApplicationId appId = super.importData(table, //
                        query, //
                        targetDir, //
                        creds, //
                        queue, //
                        jobName, //
                        splitCols, //
                        columnsToInclude, //
                        numMappers, //
                        creds.getDriverClass(), //
                        props, //
                        metadataService, //
                        hadoopConfiguration, //
                        false);
                long time2 = System.currentTimeMillis();
                log.info(String.format("Time for %s load submission = %d ms.", table, (time2 - time1)));
                return appId;
            } catch (Exception e) {
                log.error("Sqoop Import Failed! Retry " + retryCount + "\n", e);
                if (retryCount == MAX_SQOOP_RETRY) {
                    throw new LedpException(LedpCode.LEDP_12010, e, new String[] { "import" });
                }
                try {
                    Thread.sleep(RetryUtils.getExponentialWaitTime(++retryCount));
                } catch (InterruptedException e1) {
                    log.error("Sqoop Import Retry Failed! " + ExceptionUtils.getStackTrace(e1));
                }
            }
        }
        return null;
    }

    @Override
    public ApplicationId exportData(String table, String sourceDir, DbCreds creds, String queue, String customer) {
        int numDefaultMappers = hadoopConfiguration.getInt("mapreduce.map.cpu.vcores", 8);
        return exportData(table, sourceDir, creds, queue, customer, numDefaultMappers);
    }

    @Override
    public ApplicationId exportData(String table, String sourceDir, DbCreds creds, String queue, String customer,
            int numMappers) {
        final String jobName = jobNameService.createJobName(customer, "sqoop-export");
        int retryCount = 0;
        while (retryCount < MAX_SQOOP_RETRY) {
            try {
                return exportData(table, sourceDir, creds, queue, jobName, numMappers, null);
            } catch (Exception e) {
                log.error("Sqoop Export Failed! Retry " + retryCount + "\n", e);
                if (retryCount == MAX_SQOOP_RETRY) {
                    throw new LedpException(LedpCode.LEDP_12010, e, new String[] { "export" });
                }
                try {
                    Thread.sleep(RetryUtils.getExponentialWaitTime(++retryCount));
                } catch (InterruptedException e1) {
                    log.error("Sqoop Export Retry Failed! " + ExceptionUtils.getStackTrace(e1));
                }
            }
        }
        return null;
    }

    @Override
    public ApplicationId exportData(
            String table, //
            String sourceDir, DbCreds creds, String queue, String customer, int numMappers,
            String javaColumnTypeMappings) {

        String jobName = jobNameService.createJobName(customer, "sqoop-export");

        return super.exportData(table, //
                sourceDir, //
                creds, //
                queue, //
                jobName, //
                numMappers, //
                javaColumnTypeMappings, //
                null, //
                metadataService, //
                hadoopConfiguration, //
                false);
    }

    @Override
    public void eval(String sql, String assignedQueue, String jobName, DbCreds creds) {
        eval(sql, assignedQueue, jobName, metadataService.getJdbcConnectionUrl(creds));
    }

    @Override
    public void eval(String sql, String queue, String jobName, String jdbcUrl) {
        List<String> cmds = new ArrayList<>();
        cmds.add("eval");
        cmds.add("-Dmapred.job.queue.name=" + queue);
        cmds.add("--connect");
        cmds.add(jdbcUrl);
        cmds.add("--query");
        cmds.add(sql);
        LedpSqoop.runTool(cmds.toArray(new String[0]), new Configuration(hadoopConfiguration));
    }

    @Override
    public ApplicationId importDataSync(String table, String targetDir, DbCreds creds, String queue, String customer,
            List<String> splitCols, String columnsToInclude, int numMappers) {
        long time1 = System.currentTimeMillis();
        final String jobName = jobNameService.createJobName(customer, "sqoop-import");

        ApplicationId appId = super.importData(table, //
                null,//
                targetDir, //
                creds, //
                queue, //
                jobName, //
                splitCols, //
                columnsToInclude, //
                numMappers, //
                creds.getDriverClass(), //
                null, //
                metadataService, //
                hadoopConfiguration, //
                true);
        long time2 = System.currentTimeMillis();
        log.info(String.format("Time for load submission = %d ms.", (time2 - time1)));
        return appId;
    }

    @Override
    public ApplicationId importDataSyncWithWhereCondition(String table, String targetDir, DbCreds creds,
            String queue, String customer, List<String> splitCols, String columnsToInclude, String whereCondition,
            int numMappers) {
        long time1 = System.currentTimeMillis();
        final String jobName = jobNameService.createJobName(customer, "sqoop-import");

        ApplicationId appId = super.importDataWithWhereCondition(table, //
                null, //
                targetDir, //
                creds, //
                queue, //
                jobName, //
                splitCols, //
                columnsToInclude, //
                whereCondition, //
                numMappers, //
                creds.getDriverClass(), //
                null, //
                metadataService, //
                hadoopConfiguration, //
                true);
        long time2 = System.currentTimeMillis();
        log.info(String.format("Time for load submission = %d ms.", (time2 - time1)));
        return appId;
    }

    @Override
    public ApplicationId exportDataSync(String table, String sourceDir, DbCreds creds, String queue, String customer,
            int numMappers, String javaColumnTypeMappings) {
        String jobName = jobNameService.createJobName(customer, "sqoop-export");

        return exportDataSync(table, //
                sourceDir, //
                creds, //
                queue, //
                jobName, //
                numMappers, //
                javaColumnTypeMappings, //
                null);
    }

    @Override
    public ApplicationId exportDataSync(String table, String sourceDir, DbCreds creds, String queue, String customer,
            int numMappers, String javaColumnTypeMappings, String exportColumns) {
        String jobName = jobNameService.createJobName(customer, "sqoop-export");
        return super.exportData(table, //
                sourceDir, //
                creds, //
                queue, //
                jobName, //
                numMappers, //
                javaColumnTypeMappings, //
                exportColumns, //
                metadataService, //
                hadoopConfiguration, //
                true);
    }

}