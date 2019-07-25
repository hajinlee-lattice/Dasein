package com.latticeengines.datacloud.etl.ingestion.service.impl;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionProgressEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionVersionService;
import com.latticeengines.datacloud.etl.service.DataCloudEngineService;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.domain.exposed.datacloud.ingestion.SqlToSourceConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngine;
import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngineStage;

@Component("ingestionVersionService")
public class IngestionVersionServiceImpl implements IngestionVersionService, DataCloudEngineService {
    private static Logger log = LoggerFactory.getLogger(IngestionVersionServiceImpl.class);

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private IngestionEntityMgr ingestionEntityMgr;

    @Autowired
    private IngestionProgressEntityMgr ingestionProgressEntityMgr;

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private SourceService sourceService;

    @Override
    public DataCloudEngine getEngine() {
        return DataCloudEngine.INGESTION;
    }

    @Override
    public List<String> getMostRecentVersionsFromHdfs(String ingestionName, int checkVersion) {
        com.latticeengines.domain.exposed.camille.Path ingestionDir = hdfsPathBuilder
                .constructIngestionDir(ingestionName);
        List<String> versions = new ArrayList<>();
        try {
            if (HdfsUtils.isDirectory(yarnConfiguration, ingestionDir.toString())) {
                List<String> fullPaths = HdfsUtils.getFilesForDir(yarnConfiguration,
                        ingestionDir.toString());
                for (String fullPath : fullPaths) {
                    if (!fullPath.contains(HdfsPathBuilder.VERSION_FILE)
                            && fullPath.startsWith(ingestionDir.toString())) {
                        versions.add(new Path(fullPath).getName());
                    } else if (!fullPath.contains(HdfsPathBuilder.VERSION_FILE)
                            && fullPath.contains(ingestionDir.toString())) {
                        versions.add(new Path(fullPath.substring(fullPath.indexOf(ingestionDir.toString()))).getName());
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to scan hdfs directory %s", ingestionDir.toString()), e);
        }
        List<String> mostRecentVersions = new ArrayList<>();
        if (versions.size() <= checkVersion) {
            mostRecentVersions.addAll(versions);
        } else {
            Collections.sort(versions, Collections.reverseOrder());
            for (int i = 0; i < checkVersion; i++) {
                mostRecentVersions.add(versions.get(i));
            }
        }
        return mostRecentVersions;
    }

    @Override
    public String extractVersion(String timestampFormat, String str) {
        String timestampPattern = timestampFormat.replace("d", "\\d").replace("y", "\\d").replace("M", "\\d");
        Pattern pattern = Pattern.compile(timestampPattern);
        Matcher matcher = pattern.matcher(str);
        if (matcher.find()) {
            String timestampStr = matcher.group();
            DateFormat df = new SimpleDateFormat(timestampFormat);
            TimeZone timezone = TimeZone.getTimeZone("UTC");
            df.setTimeZone(timezone);
            try {
                return HdfsPathBuilder.dateFormat.format(df.parse(timestampStr));
            } catch (ParseException e) {
                throw new RuntimeException(String.format("Failed to parse timestamp %s", timestampStr), e);
            }
        } else {
            throw new RuntimeException(String.format("Failed to extract version from %s", str));
        }
    }

    @Override
    public String getFileNamePattern(String version, String fileNamePrefix, String fileNamePostfix,
            String fileExtension, String fileTimestamp) {
        String fileVersion = "";
        if (!StringUtils.isEmpty(fileTimestamp)) {
            DateFormat dateFormat = new SimpleDateFormat(HdfsPathBuilder.DATE_FORMAT_STRING);
            dateFormat.setTimeZone(TimeZone.getTimeZone(HdfsPathBuilder.UTC));
            Date timestamp = null;
            try {
                timestamp = dateFormat.parse(version);
            } catch (ParseException e) {
                throw new RuntimeException(String.format("Failed to parse version %s", version), e);
            }
            DateFormat df = new SimpleDateFormat(fileTimestamp);
            df.setTimeZone(TimeZone.getTimeZone(HdfsPathBuilder.UTC));
            fileVersion = df.format(timestamp);
        }
        return fileNamePrefix + fileVersion + fileNamePostfix + fileExtension;
    }

    @Override
    @SuppressWarnings("static-access")
    public boolean isCompleteVersion(Ingestion ingestion, String version) {
        com.latticeengines.domain.exposed.camille.Path versionPath;
        switch (ingestion.getIngestionType()) {
        case SQL_TO_SOURCE:
            SqlToSourceConfiguration sqlToSourceConfig = (SqlToSourceConfiguration) ingestion
                    .getProviderConfiguration();
            Source source = sourceService.findBySourceName(sqlToSourceConfig.getSource());
            versionPath = hdfsPathBuilder.constructTransformationSourceDir(source, version);
            break;
        default:
            versionPath = hdfsPathBuilder.constructIngestionDir(ingestion.getIngestionName(), version);
            break;
        }
        Path success = new Path(versionPath.toString(), hdfsPathBuilder.SUCCESS_FILE);
        try {
            return HdfsUtils.fileExists(yarnConfiguration, success.toString());
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to check whether ingestion %s is complete for version %s",
                    ingestion.getIngestionName(), version), e);
        }
    }

    @Override
    public void updateCurrentVersion(Ingestion ingestion, String version) {
        String currentVersion = findCurrentVersion(ingestion);
        if (currentVersion == null || currentVersion.compareTo(version) < 0) {
            setCurrentVersion(ingestion, version);
        }
    }

    @Override
    public String findCurrentVersion(String ingestionName) {
        Ingestion ingestion = ingestionEntityMgr.getIngestionByName(ingestionName);
        if (ingestion == null) {
            throw new IllegalArgumentException(String.format("Fail to find ingestion %s", ingestionName));
        }
        return findCurrentVersion(ingestion);
    }

    @Override
    public String findCurrentVersion(Ingestion ingestion) {
        switch (ingestion.getIngestionType()) {
        case SQL_TO_SOURCE:
            SqlToSourceConfiguration sqlToSourceConfig = (SqlToSourceConfiguration) ingestion
                    .getProviderConfiguration();
            return hdfsSourceEntityMgr.getCurrentVersion(sqlToSourceConfig.getSource());
        default:
            String versionFile = hdfsPathBuilder.constructVersionFile(ingestion).toString();
            String currentVersion = null;
            try {
                if (HdfsUtils.fileExists(yarnConfiguration, versionFile)) {
                    currentVersion = HdfsUtils.getHdfsFileContents(yarnConfiguration, versionFile);
                    currentVersion = StringUtils.trim(currentVersion.replace("\n", ""));
                }
            } catch (IOException e) {
                throw new RuntimeException(
                        String.format("Could not get current version of ingestion %s", ingestion.getIngestionName()),
                        e);
            }
            return currentVersion;
        }
    }

    private synchronized void setCurrentVersion(Ingestion ingestion, String version) {
        switch (ingestion.getIngestionType()) {
        case SQL_TO_SOURCE:
            SqlToSourceConfiguration sqlToSourceConfig = (SqlToSourceConfiguration) ingestion
                    .getProviderConfiguration();
            hdfsSourceEntityMgr.setCurrentVersion(sqlToSourceConfig.getSource(), version);
            break;
        default:
            String versionFile = hdfsPathBuilder.constructVersionFile(ingestion).toString();
            try {
                if (HdfsUtils.fileExists(yarnConfiguration, versionFile)) {
                    HdfsUtils.rmdir(yarnConfiguration, versionFile);
                }
                HdfsUtils.writeToFile(yarnConfiguration, versionFile, version);
                log.info(String.format("Updated current version for ingestion %s: %s", ingestion.getIngestionName(),
                        version));
            } catch (IOException e) {
                throw new RuntimeException(
                        String.format("Could not set current version of ingestion %s", ingestion.getIngestionName()),
                        e);
            }
            break;
        }

    }

    @Override
    public ProgressStatus findProgressAtVersion(String ingestionName, String version) {
        DataCloudEngineStage stage = new DataCloudEngineStage(DataCloudEngine.INGESTION, ingestionName, version);
        return findProgressAtVersion(stage).getStatus();
    }

    @Override
    public DataCloudEngineStage findProgressAtVersion(DataCloudEngineStage stage) {
        stage.setEngine(DataCloudEngine.INGESTION);
        Ingestion ingestion = ingestionEntityMgr.getIngestionByName(stage.getEngineName());
        if (ingestion == null) {
            throw new IllegalArgumentException(String.format("Fail to find ingestion %s", stage.getEngineName()));
        }
        Map<String, Object> fields = new HashMap<>();
        fields.put("IngestionId", ingestion.getPid());
        fields.put("Version", stage.getVersion());
        List<IngestionProgress> progresses = ingestionProgressEntityMgr.findProgressesByField(fields, null);
        if (CollectionUtils.isEmpty(progresses)) {
            stage.setStatus(ProgressStatus.NOTSTARTED);
            return stage;
        }
        Set<String> allJobs = new HashSet<>();
        Set<String> finishedJobs = new HashSet<>();
        Set<String> runningJobs = new HashSet<>();
        for (IngestionProgress progress : progresses) {
            if (!allJobs.contains(progress.getDestination())) {
                allJobs.add(progress.getDestination());
            }
            if (progress.getStatus() == ProgressStatus.FINISHED && !finishedJobs.contains(progress.getDestination())) {
                finishedJobs.add(progress.getDestination());
            }
            if ((progress.getStatus() == ProgressStatus.NEW || progress.getStatus() == ProgressStatus.PROCESSING
                    || (progress.getStatus() == ProgressStatus.FAILED
                            && progress.getRetries() < ingestion.getNewJobMaxRetry()))
                    && !runningJobs.contains(progress.getDestination())) {
                runningJobs.add(progress.getDestination());
            }
        }
        if (allJobs.size() == finishedJobs.size()) {
            stage.setStatus(ProgressStatus.FINISHED);
            stage.setProgress(1.0F);
        } else if (allJobs.size() == finishedJobs.size() + runningJobs.size()) {
            stage.setStatus(ProgressStatus.PROCESSING);
            stage.setProgress((float) finishedJobs.size() / allJobs.size());
        } else {
            stage.setStatus(ProgressStatus.FAILED);
            stage.setProgress((float) finishedJobs.size() / allJobs.size());
            stage.setMessage(String.format("%d ingestion jobs failed",
                    allJobs.size() - finishedJobs.size() - runningJobs.size()));
        }
        return stage;
    }

}
