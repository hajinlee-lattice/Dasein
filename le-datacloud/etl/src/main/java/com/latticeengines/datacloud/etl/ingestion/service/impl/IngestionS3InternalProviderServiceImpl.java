package com.latticeengines.datacloud.etl.ingestion.service.impl;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.yarn.LedpQueueAssigner;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionProgressEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProgressService;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionVersionService;
import com.latticeengines.domain.exposed.datacloud.ingestion.S3InternalConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.yarn.exposed.service.EMREnvService;

@Component("ingestionS3InternalProviderService")
public class IngestionS3InternalProviderServiceImpl extends IngestionProviderServiceImpl {

    private static final Logger log = LoggerFactory.getLogger(IngestionS3InternalProviderServiceImpl.class);

    @Inject
    private IngestionProgressService ingestionProgressService;

    @Inject
    private IngestionVersionService ingestionVersionService;

    @Inject
    private IngestionProgressEntityMgr ingestionProgressEntityMgr;

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @Inject
    private EMREnvService emrEnvService;

    @Inject
    private S3Service s3Service;

    @Inject
    private Configuration yarnConfiguration;

    private static final String CONTROL_FILE_NAME = "Control.json";
    // Field names in control.json file
    private static final String CONTROL_HEADER = "ControlHeader";
    private static final String TOTAL_FILE_COUNT = "totalFileCount";
    private static final String FILES = "files";
    private static final String FILE_NAME = "name";

    @Override
    public void ingest(IngestionProgress progress) {
        Ingestion ingestion = progress.getIngestion();
        S3InternalConfiguration config = (S3InternalConfiguration) ingestion.getProviderConfiguration();
        String latestVersion = progress.getVersion();
        // Go into path sourceS3Folder/latestVersion to see if control.json file
        // exists. If not present, files are not ready for ingestion
        String sourceBucket = config.getSourceBucket();
        String parentDir = config.getParentDir();
        // When force the check against control file
        if (config.getCheckControlFile()) {
            String contolObj = String.format("%s/%s/%s", parentDir, latestVersion, CONTROL_FILE_NAME);
            // When control file doesn't exist, stop the process as data is fully ready
            if (!s3Service.objectExist(sourceBucket, contolObj)) {
                log.error("Control file doesn't exist, abort ingestion process");
                ingestionProgressService.updateProgress(progress).status(ProgressStatus.FAILED).commit(true);
                return;
            }
            // Read control.json and check against it to see if data is ready
            String control = null;
            try {
                control = IOUtils.toString(s3Service.readObjectAsStream(sourceBucket, contolObj),
                        Charset.defaultCharset());
                log.info(String.format("control file content: %s", control));
            } catch (IOException e) {
                ingestionProgressService.updateProgress(progress).status(ProgressStatus.FAILED).commit(true);
                throw new RuntimeException(String.format("Fail to read control file under %s", contolObj), e);
            }
            if (!checkAgainstControlFile(sourceBucket, parentDir, latestVersion, control)) {
                log.error("Content in the source folder doesn't match with control file, abort ingestion process");
                ingestionProgressService.updateProgress(progress).status(ProgressStatus.FAILED).commit(true);
                return;
            }
        }

        // Copy raw data into destination on hdfs
        String destPath = progress.getDestination();
        String fileExtension = config.getFileExtension();
        String s3Uri = "s3a://" + sourceBucket + "/" + parentDir + "/" + latestVersion + "/*" + fileExtension;
        String queue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
        queue = LedpQueueAssigner.overwriteQueueAssignment(queue, emrEnvService.getYarnQueueScheme());
        try {
            HdfsUtils.distcp(yarnConfiguration, s3Uri, destPath, queue);
        } catch (Exception e) {
            ingestionProgressService.updateProgress(progress).status(ProgressStatus.FAILED).commit(true);
            throw new RuntimeException(String.format("Fail to copy from s3 path %s to hdfs %s", s3Uri, destPath), e);
        }

        // Update _CURRENT_VERSION file
        String sourceName = progress.getSource();
        ingestionVersionService.updateCurrentVersion(ingestion, latestVersion);

        progress = ingestionProgressService.updateProgress(progress).status(ProgressStatus.FINISHED).commit(true);
        log.info("Ingestion finished. Progress: " + progress.toString());
    }

    @Override
    public List<String> getMissingFiles(Ingestion ingestion) {
        S3InternalConfiguration config = (S3InternalConfiguration) ingestion.getProviderConfiguration();
        String sourceBucket = config.getSourceBucket();
        String parentDir = config.getParentDir();
        String latestVersion = getLatestVersionFromS3Folder(config);
        // Check if ingestion for this version is done. If so, return empty list to
        // avoid duplication since this api will be called by quartz job periodically
        String destination = hdfsPathBuilder.constructIngestionDir(ingestion.getIngestionName()).append(latestVersion)
                .toString();
        Map<String, Object> fields = new HashMap<>();
        fields.put("Source", config.getSourceNameOnHdfs());
        fields.put("Status", ProgressStatus.FINISHED);
        fields.put("Destination", destination);
        List<IngestionProgress> progresses = ingestionProgressEntityMgr.findProgressesByField(fields, null);
        if (CollectionUtils.isEmpty(progresses)) { // no existing entry in IngestionProgress table
            String sourceFolder = String.format("/%s/%s/%s", sourceBucket, parentDir, latestVersion);
            log.info(String.format("Missing files are under %s", sourceFolder));
            return Arrays.asList(sourceFolder);
        } else {
            log.info("No newer version to ingest, ignore it");
            return Collections.emptyList();
        }
    }

    private String getLatestVersionFromS3Folder(S3InternalConfiguration config) {
        String sourceBucket = config.getSourceBucket();
        String parentDir = config.getParentDir();
        String dateFormat = config.getSubfolderDateFormat();
        SimpleDateFormat formatter = new SimpleDateFormat(dateFormat);
        List<String> subfolders = s3Service.listSubFolders(sourceBucket, parentDir);
        String latestVersion = null;
        Date maxDate = new Date(0L);
        for (String folder : subfolders) {
            try {
                Date date = formatter.parse(folder);
                if (date.compareTo(maxDate) > 0) { // find newer date
                    maxDate = date;
                    latestVersion = folder;
                }
            } catch (ParseException e) {
                throw new RuntimeException(String.format("Failed to parse date string %s", folder), e);
            }
        }
        log.info(String.format("Latest version under %s/%s is %s", sourceBucket, parentDir, latestVersion));
        return latestVersion;
    }

    private Boolean checkAgainstControlFile(String sourceBucket, String parentDir, String latestVersion,
            String control) {
        if (control == null) {
            return false;
        }
        // Parse content of control.json to get totalFileCount and file list
        Set<String> fileSet = new HashSet<>();
        JsonNode jsonNode = null;
        try {
            jsonNode = new ObjectMapper().readTree(control);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Fail to parse control file");
        }
        JsonNode content = jsonNode.get(CONTROL_HEADER);
        int totalFileCount = content.get(TOTAL_FILE_COUNT).asInt();
        log.info(String.format("totalFileCount is %d", totalFileCount));
        String prefix = parentDir + "/" + latestVersion;
        content.get(FILES).elements().forEachRemaining(e -> fileSet.add(String.format("%s/%s", prefix, e.get(FILE_NAME).asText())));
        log.info("fileSet {}", fileSet);
        List<S3ObjectSummary> objects = s3Service.listObjects(sourceBucket, prefix);
        if (objects.size() - 1 != fileSet.size() || totalFileCount != objects.size() - 1) {
            log.error("File count doesn't match with control.json, abort ingestion process");
            return false;
        }
        for (S3ObjectSummary summary : objects) {
            String key = summary.getKey();
            log.error(String.format("key: %s", key));
            if (!key.equalsIgnoreCase(prefix + "/" + CONTROL_FILE_NAME) && !fileSet.contains(key)) {
                return false;
            }
        }
        return true;
    }
}
