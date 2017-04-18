package com.latticeengines.sqoop.service.impl;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.v2.app.MRAppMaster;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.sqoop.LedpSqoop;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.db.exposed.service.DbMetadataService;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.sqoop.exposed.service.SqoopJobService;

@SuppressWarnings("deprecation")
@Component("sqoopJobService")
public class SqoopJobServiceImpl implements SqoopJobService {

    private static final Log log = LogFactory.getLog(SqoopJobServiceImpl.class);

    private static final int MAX_SQOOP_RETRY = 3;

    @Value("${dataplatform.queue.scheme}")
    private String queueScheme;

    @Value("${dataplatform.hdfs.stack:}")
    private String stackName;

    @Autowired
    protected VersionManager versionManager;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private DbMetadataService dbMetadataService;

    public ApplicationId exportData(SqoopExporter exporter) {
        Configuration yarnConfiguration = new Configuration(this.yarnConfiguration);
        int numMappers = exporter.getNumMappers();
        if (numMappers < 1) {
            numMappers = yarnConfiguration.getInt("mapreduce.map.cpu.vcores", 8);
        }

        List<String> cmds = new ArrayList<>();
        cmds.add("export");
        if (exporter.getHadoopArgs() != null) {
            for (String option : exporter.getHadoopArgs()) {
                cmds.add(overwriteQueueInHadoopOpt(option));
            }
        }
        cmds.add("--connect");
        cmds.add(dbMetadataService.getConnectionUrl(exporter.getDbCreds()));
        cmds.add("--username");
        cmds.add(dbMetadataService.getConnectionUserName(exporter.getDbCreds()));
        cmds.add("--password");
        cmds.add(dbMetadataService.getConnectionPassword(exporter.getDbCreds()));
        cmds.add("--table");
        cmds.add(exporter.getTable());
        cmds.add("--mapreduce-job-name");
        cmds.add(exporter.fullJobName());
        cmds.add("--export-dir");
        cmds.add(exporter.getSourceDir());
        cmds.add("--num-mappers");
        cmds.add(Integer.toString(numMappers));
        String uuid = UUID.randomUUID().toString();
        cmds.add("--bindir");
        cmds.add(getBinaryInputDir(uuid));
        cmds.add("--outdir");
        cmds.add(getGenerateOutputDir(uuid));
        if (StringUtils.isNotEmpty(exporter.getJavaColumnTypeMappings())) {
            cmds.add("--map-column-java");
            cmds.add(exporter.getJavaColumnTypeMappings());
        }
        if (exporter.getExportColumns() != null && !exporter.getExportColumns().isEmpty()) {
            cmds.add("--columns");
            cmds.add(StringUtils.join(exporter.getExportColumns(), ","));
        }
        if (exporter.getOtherOptions() != null) {
            for (String option : exporter.getOtherOptions()) {
                cmds.add(option);
            }
        }
        addShadedJarToDistributedCache(yarnConfiguration);
        yarnConfiguration.set("yarn.mr.am.class.name", MRAppMaster.class.getName());
        try {
            return runTool(cmds, yarnConfiguration, exporter.isSync(), uuid);
        } finally {
            FileUtils.deleteQuietly(new File(getGenerateOutputDir(uuid)));
            FileUtils.deleteQuietly(new File(getBinaryInputDir(uuid)));
        }
    }

    public ApplicationId importData(SqoopImporter importer) {
        Configuration yarnConfiguration = new Configuration(this.yarnConfiguration);

        boolean targeDirExists = false;
        try {
            targeDirExists = HdfsUtils.fileExists(yarnConfiguration, importer.getTargetDir());
        } catch (Exception e) {
            throw new RuntimeException("Failed to check existence of target dir " + importer.getTargetDir(), e);
        }

        if (targeDirExists) {
            throw new IllegalStateException("Target folder " + importer.getTargetDir()
                    + " already exists. Please remove it before invoking sqoop import.");
        }

        int numMappers = importer.getNumMappers();
        if (numMappers < 1) {
            numMappers = yarnConfiguration.getInt("mapreduce.map.cpu.vcores", 8);
        }

        String table = importer.getTable();
        if (importer.getSplitColumn() == null || importer.getSplitColumn().isEmpty()
                || (StringUtils.isNotEmpty(table) && table.startsWith("Play"))) {
            numMappers = 1;
        }

        List<String> cmds = new ArrayList<>();
        cmds.add("import");
        if (importer.getHadoopArgs() != null) {
            for (String option : importer.getHadoopArgs()) {
                cmds.add(overwriteQueueInHadoopOpt(option));
            }
        }
        cmds.add("--connect");
        cmds.add(dbMetadataService.getConnectionUrl(importer.getDbCreds()));
        cmds.add("--username");
        cmds.add(dbMetadataService.getConnectionUserName(importer.getDbCreds()));
        cmds.add("--password");
        cmds.add(dbMetadataService.getConnectionPassword(importer.getDbCreds()));

        if (SqoopImporter.Mode.TABLE.equals(importer.getMode())) {
            cmds.add("--table");
            cmds.add(table);
        } else {
            cmds.add("--query");
            cmds.add(importer.getQuery());
        }
        cmds.add("--mapreduce-job-name");
        cmds.add(importer.fullJobName());
        if (importer.getColumnsToInclude() != null && !importer.getColumnsToInclude().isEmpty()) {
            cmds.add("--columns");
            cmds.add(StringUtils.join(importer.getColumnsToInclude(), ","));
        }
        if (StringUtils.isNotEmpty(importer.getDbCreds().getDriverClass())) {
            cmds.add("--driver");
            cmds.add(importer.getDbCreds().getDriverClass());
        }
        if (StringUtils.isNotEmpty(importer.getSplitColumn())) {
            cmds.add("--split-by");
            cmds.add(importer.getSplitColumn());
        }
        cmds.add("--num-mappers");
        cmds.add(Integer.toString(numMappers));
        cmds.add("--target-dir");
        cmds.add(importer.getTargetDir());
        String uuid = UUID.randomUUID().toString();
        cmds.add("--bindir");
        cmds.add(getBinaryInputDir(uuid));
        cmds.add("--outdir");
        cmds.add(getGenerateOutputDir(uuid));

        if (importer.getOtherOptions() != null) {
            for (String option : importer.getOtherOptions()) {
                cmds.add(option);
            }
        }

        String propsFileName = null;
        if (importer.getProperties() != null) {
            propsFileName = String.format("sqoop-import-props-%s.properties", System.currentTimeMillis());
            File propsFile = new File(propsFileName);
            try {
                importer.getProperties().store(new FileWriter(propsFile), "");
                cmds.add("--connection-param-file");
                cmds.add(propsFile.getCanonicalPath());
            } catch (IOException e) {
                log.error(e);
            }
            String hdfsClassPath = importer.getProperties().getProperty("yarn.mr.hdfs.class.path");

            if (hdfsClassPath != null) {
                yarnConfiguration.set("yarn.mr.hdfs.class.path", hdfsClassPath);
            }

            String hdfsResources = importer.getProperties().getProperty("yarn.mr.hdfs.resources");

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
        addShadedJarToDistributedCache(yarnConfiguration);
        yarnConfiguration.set("yarn.mr.am.class.name", MRAppMaster.class.getName());

        try {
            return runTool(cmds, yarnConfiguration, importer.isSync(), uuid);
        } finally {
            FileUtils.deleteQuietly(new File(getGenerateOutputDir(uuid)));
            FileUtils.deleteQuietly(new File(getBinaryInputDir(uuid)));
            if (StringUtils.isNotEmpty(propsFileName)) {
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
        config.setBoolean("mapreduce.job.user.classpath.first", true);
        int retryCount = 0;
        while (retryCount < MAX_SQOOP_RETRY) {
            try {
                LedpSqoop.runTool(cmds.toArray(new String[0]), new Configuration(config));
                return getApplicationId(appIdFilePath);
            } catch (Exception e) {
                log.error("Sqoop Job Failed! Retry " + retryCount + "\n", e);
                if (retryCount == MAX_SQOOP_RETRY) {
                    throw new LedpException(LedpCode.LEDP_12010, e, new String[] { "finish" });
                }
                try {
                    Thread.sleep(RetryUtils.getExponentialWaitTime(++retryCount));
                } catch (InterruptedException e1) {
                    log.error("Sqoop Job Retry Failed! " + ExceptionUtils.getStackTrace(e1));
                }
            }
        }
        return null;
    }

    private String getBinaryInputDir(String uuid) {
        return "/tmp/sqoop-yarn/compile/indir/" + uuid;
    }

    private String getGenerateOutputDir(String uuid) {
        return "/tmp/sqoop-yarn/generate/outdir/" + uuid;
    }

    private void addShadedJarToDistributedCache(Configuration yarnConfiguration) {
        List<String> jarFilePaths = getPlatformShadedJarPathList(yarnConfiguration,
                versionManager.getCurrentVersionInStack(stackName));
        for (String jarFilePath : jarFilePaths) {
            try {
                DistributedCache.addCacheFile(new URI(jarFilePath), yarnConfiguration);
            } catch (URISyntaxException e) {
                log.error(e);
                throw new LedpException(LedpCode.LEDP_00002);
            }
        }
    }

    private static List<String> getPlatformShadedJarPathList(Configuration yarnConfiguration, String version) {
        try {
            return HdfsUtils.getFilesForDir(yarnConfiguration, String.format("/app/%s/sqoop/lib", version), ".*.jar$");
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
    }

    protected String overwriteQueue(String queue) {
        if (StringUtils.isEmpty(queue)) {
            queue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
        }
        return LedpQueueAssigner.overwriteQueueAssignment(queue, queueScheme);
    }

    protected String overwriteQueueInHadoopOpt(String hadoopOpt) {
        String[] tokens = hadoopOpt.split("=");
        if (tokens[0].contains("mapreduce.job.queuename")) {
            String queue = overwriteQueue(tokens[1]);
            return "-Dmapreduce.job.queuename=" + queue;
        } else {
            return hadoopOpt;
        }
    }

}
