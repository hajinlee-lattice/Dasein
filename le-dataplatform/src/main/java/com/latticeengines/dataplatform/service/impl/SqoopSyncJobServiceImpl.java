package com.latticeengines.dataplatform.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.sqoop.LedpSqoop;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.yarn.client.YarnClient;

import com.latticeengines.dataplatform.exposed.service.JobNameService;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.dataplatform.exposed.service.SqoopSyncJobService;
import com.latticeengines.domain.exposed.modeling.DbCreds;

@Component("sqoopSyncJobService")
public class SqoopSyncJobServiceImpl extends SqoopJobServiceImpl implements SqoopSyncJobService {

    @Autowired
    private Configuration hadoopConfiguration;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private JobNameService jobNameService;

    @Autowired
    private MetadataService metadataService;

    @Autowired
    protected YarnClient defaultYarnClient;

    protected static final int MAX_TRIES = 60;
    protected static final long APP_WAIT_TIME = 1000L;

    protected static final Log log = LogFactory.getLog(SqoopSyncJobServiceImpl.class);

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
    public ApplicationId importData(String table, String targetDir, DbCreds creds, String queue, String customer,
            List<String> splitCols, String columnsToInclude, int numMappers, Properties props) {
        long time1 = System.currentTimeMillis();
        final String jobName = jobNameService.createJobName(customer, "sqoop-import");

        ApplicationId appId = super.importData(table, // 
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
                yarnConfiguration, //
                false);
        long time2 = System.currentTimeMillis();
        log.info(String.format("Time for load submission = %d ms.", (time2 - time1)));
        return appId;
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
        return exportData(table, sourceDir, creds, queue, jobName, numMappers, null);
    }

    @Override
    public ApplicationId exportData(String table, //
            String sourceDir, DbCreds creds, String queue, String customer,
            int numMappers, String javaColumnTypeMappings) {

        String jobName = jobNameService.createJobName(customer, "sqoop-export");

        return super.exportData(table, //
                sourceDir, //
                creds, //
                queue, //
                jobName, //
                numMappers, //
                javaColumnTypeMappings, //
                metadataService, //
                yarnConfiguration, //
                false);
    }

    @Override
    public void eval(String sql, String queue, String jobName, int numMappers, String jdbcUrl) {
        List<String> cmds = new ArrayList<>();
        cmds.add("eval");
        cmds.add("-Dmapred.job.queue.name=" + queue);
        cmds.add("--connect");
        cmds.add(jdbcUrl);
        cmds.add("--query");
        cmds.add(sql);
        LedpSqoop.runTool(cmds.toArray(new String[0]), new Configuration(yarnConfiguration));
    }

    @Override
    public ApplicationId importDataSync(String table, String targetDir, DbCreds creds, String queue, String customer,
            List<String> splitCols, String columnsToInclude, int numMappers) {
        long time1 = System.currentTimeMillis();
        final String jobName = jobNameService.createJobName(customer, "sqoop-import");

        ApplicationId appId = super.importData(table, // 
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
                yarnConfiguration, //
                true);
        long time2 = System.currentTimeMillis();
        log.info(String.format("Time for load submission = %d ms.", (time2 - time1)));
        return appId;
    }

    @Override
    public ApplicationId exportDataSync(String table, String sourceDir, DbCreds creds, String queue, String customer,
            int numMappers, String javaColumnTypeMappings) {
        String jobName = jobNameService.createJobName(customer, "sqoop-export");

        return super.exportData(table, //
                sourceDir, //
                creds, //
                queue, //
                jobName, //
                numMappers, //
                javaColumnTypeMappings, //
                metadataService, //
                yarnConfiguration, //
                true);
    }

}
