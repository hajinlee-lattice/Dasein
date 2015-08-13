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

import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.domain.exposed.modeling.DbCreds;

public class SqoopJobServiceImpl {
    
    private static final Log log = LogFactory.getLog(SqoopJobServiceImpl.class);

    protected ApplicationId exportData(String table, //
            String sourceDir, //
            DbCreds creds, //
            String queue, //
            String jobName, //
            int numMappers, //
            String javaColumnTypeMappings, //
            String exportColumns, //
            MetadataService metadataService, //
            Configuration yarnConfiguration, //
            boolean sync) {
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
        return runTool(cmds, yarnConfiguration, sync);
    }


    protected ApplicationId importData(String table, //
            String targetDir, //
            DbCreds creds, //
            String queue, //
            String jobName, //
            List<String> splitCols, //
            String columnsToInclude, //
            int numMappers, //
            String driver, //
            Properties props, //
            MetadataService metadataService, //
            Configuration yarnConfiguration, //
            boolean sync) {

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
        String propsFileName = null;
        if (props != null) {
            propsFileName = String.format("sqoop-import-props-%s.properties", System.currentTimeMillis());
            File propsFile = new File(propsFileName);
            try {
                props.store(new FileWriter(propsFile), "");
                cmds.add("--connection-param-file");
                cmds.add(propsFile.getCanonicalPath());
            } catch (IOException e) {
                log.error(e);
            }
            String hdfsClassPath = props.getProperty("yarn.mr.hdfs.class.path");
            
            if (hdfsClassPath != null) {
                yarnConfiguration.set("yarn.mr.hdfs.class.path", hdfsClassPath);
            }
        }
        yarnConfiguration.set("yarn.mr.am.class.name", LedpMRAppMaster.class.getName());
        //yarnConfiguration.set(MRJobConfig.MR_AM_COMMAND_OPTS, "-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=4001,server=y,suspend=y");
                
        try {
            return runTool(cmds, yarnConfiguration, sync);
        } finally {
            FileUtils.deleteQuietly(new File(table + ".avsc"));
            FileUtils.deleteQuietly(new File(table + ".java"));
            
            if (propsFileName != null) {
                FileUtils.deleteQuietly(new File(propsFileName));
            }
        }
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

    private ApplicationId runTool(List<String> cmds, Configuration config, boolean sync) {
        String appIdFileName = String.format("appid-%s.txt", UUID.randomUUID().toString());
        
        config.set("sqoop.sync", Boolean.toString(sync));
        config.set("sqoop.app.id.file.name", appIdFileName);
        LedpSqoop.runTool(cmds.toArray(new String[0]), new Configuration(config));
        return getApplicationId(appIdFileName);
    }


}
