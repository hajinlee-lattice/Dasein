package com.latticeengines.dataplatform.service.impl;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
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
        cmds.add("-Dmapreduce.job.queuename=" + queue);
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
        String uuid = UUID.randomUUID().toString();
        cmds.add("--bindir");
        cmds.add(getBinaryInputDir(uuid));
        cmds.add("--outdir");
        cmds.add(getGenerateOutputDir(uuid));
        if (javaColumnTypeMappings != null) {
            cmds.add("--map-column-java");
            cmds.add(javaColumnTypeMappings);
        }
        if (exportColumns != null) {
            cmds.add("--columns");
            cmds.add(exportColumns);
        }
        try {
            return runTool(cmds, yarnConfiguration, sync, uuid);
        } finally {
            FileUtils.deleteQuietly(new File(getGenerateOutputDir(uuid)));
            FileUtils.deleteQuietly(new File(getBinaryInputDir(uuid)));
        }
    }

    protected ApplicationId importData(String table, //
            String query, //
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

        return importDataWithWhereCondition(
                table, query, targetDir, creds, queue, jobName, splitCols, //
                columnsToInclude, "", numMappers, driver, props, //
                metadataService, yarnConfiguration, sync
        );
    }

    @SuppressWarnings("deprecation")
    protected ApplicationId importDataWithWhereCondition(String table, //
                                       String query, //
                                       String targetDir, //
                                       DbCreds creds, //
                                       String queue, //
                                       String jobName, //
                                       List<String> splitCols, //
                                       String columnsToInclude, //
                                       String whereCondition, //
                                       int numMappers, //
                                       String driver, //
                                       Properties props, //
                                       MetadataService metadataService, //
                                       Configuration yarnConfiguration, //
                                       boolean sync) {

        if (table != null && table.startsWith("Play")) {
            numMappers = 1;
        }

        List<String> cmds = new ArrayList<>();
        cmds.add("import");
        cmds.add("-Dmapreduce.job.queuename=" + queue);
        cmds.add("--connect");
        cmds.add(metadataService.getJdbcConnectionUrl(creds));
        cmds.add("--m");
        cmds.add(Integer.toString(numMappers));
        
        if (query == null) {
            cmds.add("--table");
            cmds.add(table);
        } else {
            cmds.add("--query");
            cmds.add(query);
        }
        cmds.add("--as-avrodatafile");
        cmds.add("--compress");
        cmds.add("--mapreduce-job-name");
        cmds.add(jobName);
        if (columnsToInclude != null && !columnsToInclude.isEmpty()) {
            cmds.add("--columns");
            cmds.add(columnsToInclude);
        }
        if (StringUtils.isNotEmpty(whereCondition)) {
            cmds.add("--where");
            cmds.add(whereCondition);
        }
        if (driver != null && !driver.isEmpty()) {
            cmds.add("--driver");
            cmds.add(driver);
        }
        cmds.add("--split-by");
        cmds.add(StringUtils.join(splitCols, ","));
        cmds.add("--target-dir");
        cmds.add(targetDir);
        String uuid = UUID.randomUUID().toString();
        cmds.add("--bindir");
        cmds.add(getBinaryInputDir(uuid));
        cmds.add("--outdir");
        cmds.add(getGenerateOutputDir(uuid));
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
            
            String hdfsResources = props.getProperty("yarn.mr.hdfs.resources");
            
            if (hdfsResources != null) {
                String[] hdfsResourceList = hdfsResources.split(",");
                
                for (String hdfsResource : hdfsResourceList) {
                    try {
                        DistributedCache.addCacheFile(new URI(hdfsResource), yarnConfiguration);
                    } catch (URISyntaxException e) {
                        log.error(e);
                    }
                }
            }
        }
        yarnConfiguration.set("yarn.mr.am.class.name", LedpMRAppMaster.class.getName());
        // yarnConfiguration.set(MRJobConfig.MR_AM_COMMAND_OPTS,
        // "-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=4001,server=y,suspend=y");

        try {
            return runTool(cmds, yarnConfiguration, sync, uuid);
        } finally {
            FileUtils.deleteQuietly(new File(getGenerateOutputDir(uuid)));
            FileUtils.deleteQuietly(new File(getBinaryInputDir(uuid)));
            if (propsFileName != null) {
                FileUtils.deleteQuietly(new File(propsFileName));
            }
        }
    }

    private ApplicationId getApplicationId(final String appIdFilePath) {
        String jobId = null;
        File appIdFile = new File(appIdFilePath);
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

    private ApplicationId runTool(List<String> cmds, Configuration config, boolean sync, String uuid) {
        String appIdFileName = String.format("appid-%s.txt", UUID.randomUUID().toString());
        String appIdFilePath = getBinaryInputDir(uuid) + "/" + appIdFileName;
        config.set("sqoop.sync", Boolean.toString(sync));
        config.set("sqoop.app.id.file.name", appIdFilePath);
        LedpSqoop.runTool(cmds.toArray(new String[0]), new Configuration(config));
        return getApplicationId(appIdFilePath);
    }

    private String getBinaryInputDir(String uuid) {
        return "/tmp/sqoop-yarn/compile/indir/" + uuid;
    }

    private String getGenerateOutputDir(String uuid) {
        return "/tmp/sqoop-yarn/generate/outdir/" + uuid;
    }

}