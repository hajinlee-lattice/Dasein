package com.latticeengines.jobs.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.sqoop.LedpSqoop;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.service.JobNameService;
import com.latticeengines.dataplatform.service.impl.JobServiceImpl;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.jobs.PropDataJobService;

@Component("propDataJobService")
public class PropDataJobServiceImpl extends JobServiceImpl implements PropDataJobService {

    @Autowired
    private JobNameService jobNameService;

    @Override
    public ApplicationId importData(String table, String targetDir, String queue, String customer, String splitCols,
            String jdbcUrl) {
        int numDefaultMappers = hadoopConfiguration.getInt("mapreduce.map.cpu.vcores", 8);
        return importData(table, targetDir, queue, customer, splitCols, numDefaultMappers, jdbcUrl);
    }

    @Override
    public ApplicationId importData(String table, String targetDir, String queue, String customer, String splitCols,
            int numMappers, String jdbcUrl) {

        final String jobName = jobNameService.createJobName(customer, "propdata-import");

        importSync(table, targetDir, queue, jobName, splitCols, numMappers, jdbcUrl);

        return getApplicationId(jobName);
    }

    private ApplicationId getApplicationId(final String jobName) {
        int tries = 0;
        ApplicationId appId = null;
        while (tries < MAX_TRIES) {
            try {
                Thread.sleep(APP_WAIT_TIME);
            } catch (InterruptedException e) {
                log.warn("Thread.sleep interrupted.", e);
            }
            appId = getAppIdFromName(jobName);
            if (appId != null) {
                return appId;
            }
            tries++;
        }

        return appId;

    }

    private void importSync(final String table, final String targetDir, final String queue, final String jobName,
            final String splitCols, final int numMappers, String jdbcUrl) {
        List<String> cmds = new ArrayList<>();
        cmds.add("import");
        cmds.add("-Dmapred.job.queue.name=" + queue);
        cmds.add("--connect");
        cmds.add(jdbcUrl);
        cmds.add("--m");
        cmds.add(Integer.toString(numMappers));
        cmds.add("--table");
        cmds.add(table);
        cmds.add("--as-avrodatafile");
        cmds.add("--compress");
        cmds.add("--mapreduce-job-name");
        cmds.add(jobName);
        cmds.add("--split-by");
        cmds.add(splitCols);
        cmds.add("--target-dir");
        cmds.add(targetDir);
        LedpSqoop.runTool(cmds.toArray(new String[0]), new Configuration(yarnConfiguration));

    }

    @Override
    public JobStatus getJobStatus(String applicationId) {
        return null;
    }

    @Override
    public ApplicationId exportData(String table, String sourceDir, String queue, String customer, String jdbcUrl) {

        int numDefaultMappers = hadoopConfiguration.getInt("mapreduce.map.cpu.vcores", 8);
        return exportData(table, sourceDir, queue, customer, numDefaultMappers, jdbcUrl);
    }

    @Override
    public ApplicationId exportData(String table, String sourceDir, String queue, String customer, int numMappers,
            String jdbcUrl) {

        final String jobName = jobNameService.createJobName(customer, "propdata-export");

        exportSync(table, sourceDir, queue, jobName, numMappers, jdbcUrl, null);

        return getApplicationId(jobName);
    }

    @Override
    public ApplicationId exportData(String table, String sourceDir, String queue, String customer, int numMappers,
            String jdbcUrl, String javaColumnTypeMappings) {

        final String jobName = jobNameService.createJobName(customer, "propdata-export");

        exportSync(table, sourceDir, queue, jobName, numMappers, jdbcUrl, javaColumnTypeMappings);

        return getApplicationId(jobName);
    }
    
    private void exportSync(final String table, final String sourceDir, final String queue, final String jobName,
            final int numMappers, String jdbcUrl, String javaColumnTypeMappings) {
        List<String> cmds = new ArrayList<>();
        cmds.add("export");
        cmds.add("-Dmapred.job.queue.name=" + queue);
        cmds.add("--connect");
        cmds.add(jdbcUrl);
        cmds.add("--m");
        cmds.add(Integer.toString(numMappers));
        cmds.add("--table");
        cmds.add(table);
        cmds.add("--direct");
        cmds.add("--mapreduce-job-name");
        cmds.add(jobName);
        cmds.add("--export-dir");
        cmds.add(sourceDir);
        if (javaColumnTypeMappings != null) {
            cmds.add("--map-column-java");
            cmds.add(javaColumnTypeMappings);
        }
        LedpSqoop.runTool(cmds.toArray(new String[0]), new Configuration(yarnConfiguration));
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

}
