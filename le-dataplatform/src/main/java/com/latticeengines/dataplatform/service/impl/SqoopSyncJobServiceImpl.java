package com.latticeengines.dataplatform.service.impl;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.app.LedpMRAppMaster;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.sqoop.LedpSqoop;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.yarn.client.YarnClient;

import com.latticeengines.dataplatform.exposed.service.JobNameService;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.dataplatform.exposed.service.SqoopSyncJobService;
import com.latticeengines.domain.exposed.modeling.DbCreds;

@Component("sqoopSyncJobService")
public class SqoopSyncJobServiceImpl implements SqoopSyncJobService {

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
            List<String> splitCols, String columnsToInclude) {
        int numDefaultMappers = hadoopConfiguration.getInt("mapreduce.map.cpu.vcores", 8);
        return importData(table, targetDir, creds, queue, customer, splitCols, columnsToInclude, numDefaultMappers, null);
    }
    
    @Override
    public ApplicationId importData(String table, String targetDir, DbCreds creds, String queue, String customer,
            List<String> splitCols, String columnsToInclude, int numMappers) {
        return importData(table, targetDir, creds, queue, customer, splitCols, columnsToInclude, numMappers, null);
    }

    @Override
    public ApplicationId importData(String table, String targetDir, DbCreds creds, String queue, String customer,
            List<String> splitCols, String columnsToInclude, int numMappers, Properties props) {

        final String jobName = jobNameService.createJobName(customer, "sqoop-import");

        String appIdFileName = importSync(table, targetDir, creds, queue, jobName, splitCols, columnsToInclude, creds.getDriverClass(), numMappers, props);

        return getApplicationId(appIdFileName);
    }

    private ApplicationId getApplicationId(final String appIdFileName) {
        String jobId = null;
        File appIdFile = new File(appIdFileName);
        try {
            jobId = FileUtils.readFileToString(appIdFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String[] idComponents = jobId.split("_");
        try {
            return ApplicationIdPBImpl.newInstance(Long.parseLong(idComponents[1]), Integer.parseInt(idComponents[2]));
        } finally {
            FileUtils.deleteQuietly(appIdFile);
        }
        
    }

    private String importSync(final String table, final String targetDir, final DbCreds creds, final String queue,
            final String jobName, final List<String> splitCols, final String columnsToInclude, String driver, 
            int numMappers, final Properties props) {
        if (table.startsWith("Play")) {
            numMappers = 1;
        }

        List<String> cmds = new ArrayList<>();
        cmds.add("import");
        cmds.add("-Dmapred.job.queue.name=" + queue);
        cmds.add("--connect");
        cmds.add(metadataService.getJdbcConnectionUrl(creds));
        cmds.add("--m");
        cmds.add(Integer.toString(numMappers));
        cmds.add("--table");
        cmds.add(table);
        cmds.add("--as-avrodatafile");
        cmds.add("--compress");
        cmds.add("--mapreduce-job-name");
        cmds.add(jobName);
        if (columnsToInclude != null && !columnsToInclude.isEmpty()) {
            cmds.add("--columns");
            cmds.add(columnsToInclude);
        }
        if (driver != null && !driver.isEmpty()) {
            cmds.add("--driver");
            cmds.add(driver);
        }
        cmds.add("--split-by");
        cmds.add(StringUtils.join(splitCols, ","));
        cmds.add("--target-dir");
        cmds.add(targetDir);
        
        if (props != null) {
            String propsFileName = String.format("sqoop-import-props-%s.properties", System.currentTimeMillis());
            File propsFile = new File(propsFileName);
            try {
                props.store(new FileWriter(propsFile), "");
                cmds.add("--connection-param-file");
                cmds.add(propsFile.getCanonicalPath());
            } catch (IOException e) {
                log.error(e);
            }
        }
        yarnConfiguration.set("yarn.mr.am.class.name", LedpMRAppMaster.class.getName());
        String appIdFileName = String.format("appid-%s.txt", UUID.randomUUID().toString());
        yarnConfiguration.set("sqoop.app.id.file.name", appIdFileName);
        // yarnConfiguration.set(MRJobConfig.MR_AM_COMMAND_OPTS,
        // "-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=5003,server=y,suspend=y");
        Configuration config = new Configuration(yarnConfiguration);
        try {
            LedpSqoop.runTool(cmds.toArray(new String[0]), config);
        } finally {
            FileUtils.deleteQuietly(new File(table + ".avsc"));
            FileUtils.deleteQuietly(new File(table + ".java"));
        }
        
        return appIdFileName;
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

        exportSync(table, sourceDir, creds, queue, jobName, numMappers, null, null);

        return getApplicationId(jobName);
    }

    @Override
    public ApplicationId exportData(String table, String sourceDir, DbCreds creds, String queue, String customer,
            int numMappers, String javaColumnTypeMappings) {

        final String jobName = jobNameService.createJobName(customer, "sqoop-export");

        exportSync(table, sourceDir, creds, queue, jobName, numMappers, javaColumnTypeMappings, null);

        return getApplicationId(jobName);
    }

    @Override
    public ApplicationId exportData(String table, String sourceDir, DbCreds creds, String queue, String customer,
            int numMappers, String javaColumnTypeMappings, String exportColumns) {

        final String jobName = jobNameService.createJobName(customer, "sqoop-export");

        exportSync(table, sourceDir, creds, queue, jobName, numMappers, javaColumnTypeMappings, exportColumns);

        return getApplicationId(jobName);
    }

    private void exportSync(final String table, final String sourceDir, final DbCreds creds, final String queue,
            final String jobName, final int numMappers, String javaColumnTypeMappings, String exportColumns) {
        List<String> cmds = new ArrayList<>();
        cmds.add("export");
        cmds.add("-Dmapred.job.queue.name=" + queue);
        cmds.add("--connect");
        cmds.add(metadataService.getJdbcConnectionUrl(creds));
        cmds.add("--m");
        cmds.add(Integer.toString(numMappers));
        cmds.add("--table");
        cmds.add(table);
        cmds.add("--mapreduce-job-name");
        cmds.add(jobName);
        cmds.add("--export-dir");
        cmds.add(sourceDir);
        if (javaColumnTypeMappings != null) {
            cmds.add("--map-column-java");
            cmds.add(javaColumnTypeMappings);
        }
        if (exportColumns != null) {
            cmds.add("--columns");
            cmds.add(exportColumns);
        }
        LedpSqoop.runTool(cmds.toArray(new String[0]), new Configuration(yarnConfiguration));
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
        LedpSqoop.runTool(cmds.toArray(new String[0]), new Configuration(yarnConfiguration));
    }

}
