package com.latticeengines.datacloud.etl.ingestion.service.impl;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionProgressEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionAPIProviderService;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProgressService;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProgressUpdater;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionVersionService;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.domain.exposed.datacloud.ingestion.ApiConfiguration;
import com.latticeengines.domain.exposed.datacloud.ingestion.BWRawDestination;
import com.latticeengines.domain.exposed.datacloud.ingestion.S3Configuration;
import com.latticeengines.domain.exposed.datacloud.ingestion.S3Destination;
import com.latticeengines.domain.exposed.datacloud.ingestion.SftpConfiguration;
import com.latticeengines.domain.exposed.datacloud.ingestion.SqlToSourceConfiguration;
import com.latticeengines.domain.exposed.datacloud.ingestion.SqlToTextConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.eai.route.CamelRouteConfiguration;
import com.latticeengines.domain.exposed.eai.route.SftpToHdfsRouteConfiguration;

@Component("ingestionProgressService")
public class IngestionProgressServiceImpl implements IngestionProgressService {

    private static final Logger log = LoggerFactory.getLogger(IngestionProgressServiceImpl.class);


    @Autowired
    private IngestionProgressEntityMgr ingestionProgressEntityMgr;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private IngestionAPIProviderService apiProviderService;

    @Autowired
    private SourceService sourceService;

    @Autowired
    private IngestionVersionService ingestionVersionService;

    @Value("${datacloud.collection.s3bucket}")
    private String defaultBucket;

    @Override
    public List<IngestionProgress> getProgressesByField(Map<String, Object> fields, List<String> orderFields) {
        return ingestionProgressEntityMgr.findProgressesByField(fields, orderFields);
    }

    @Override
    public IngestionProgress createDraftProgress(Ingestion ingestion, String triggeredBy, String file, String version) {
        IngestionProgress progress = initProgress(ingestion, triggeredBy);
        inflateProgress(progress, ingestion, file, version);
        return progress;
    }

    private IngestionProgress initProgress(Ingestion ingestion, String triggeredBy) {
        IngestionProgress progress = new IngestionProgress();
        progress.setHdfsPod(HdfsPodContext.getHdfsPodId());
        progress.setIngestion(ingestion);
        progress.setLatestStatusUpdate(new Date());
        progress.setRetries(0);
        progress.setStatus(ProgressStatus.NEW);
        progress.setTriggeredBy(triggeredBy);
        return progress;
    }

    private void inflateProgress(IngestionProgress progress, Ingestion ingestion, String file, String version) {
        com.latticeengines.domain.exposed.camille.Path ingestionDir = hdfsPathBuilder
                .constructIngestionDir(ingestion.getIngestionName());
        switch (ingestion.getIngestionType()) {
        case SFTP:
            SftpConfiguration sftpConfig = (SftpConfiguration) ingestion.getProviderConfiguration();
            String fileName = new Path(file).getName();
            String source = StringUtils.isBlank(sftpConfig.getSftpDir()) ? file
                    : new Path(sftpConfig.getSftpDir(), file).toString();
            progress.setSource(source);
            // Arbitrary set version will not be respected
            progress.setVersion(ingestionVersionService.extractVersion(sftpConfig.getFileTimestamp(), fileName));
            progress.setDestination(ingestionDir.append(progress.getVersion()).append(fileName).toString());
            break;
        case API:
            ApiConfiguration apiConfig = (ApiConfiguration) ingestion.getProviderConfiguration();
            progress.setSource(apiConfig.getFileUrl());
            // Arbitrary set version will not be respected
            progress.setVersion(apiProviderService.getTargetVersion(apiConfig));
            progress.setDestination(
                    ingestionDir.append(progress.getVersion()).append(apiConfig.getFileName()).toString());
            break;
        case SQL_TO_CSVGZ:
            SqlToTextConfiguration sqlToTextConfig = (SqlToTextConfiguration) ingestion
                    .getProviderConfiguration();
            progress.setSource(sqlToTextConfig.getDbTable());
            if (StringUtils.isBlank(version)) {
                progress.setVersion(HdfsPathBuilder.dateFormat.format(new Date()));
            } else {
                progress.setVersion(version);
            }
            progress.setDestination(ingestionDir.append(progress.getVersion()).append(file).toString());
            break;
        case SQL_TO_SOURCE:
            SqlToSourceConfiguration sqlToSourceConfig = (SqlToSourceConfiguration) ingestion
                    .getProviderConfiguration();
            progress.setSource(sqlToSourceConfig.getDbTable());
            if (StringUtils.isBlank(version)) {
                progress.setVersion(HdfsPathBuilder.dateFormat.format(new Date()));
            } else {
                progress.setVersion(version);
            }
            progress.setDestination(hdfsPathBuilder.constructTransformationSourceDir(
                    sourceService.findBySourceName(sqlToSourceConfig.getSource()), progress.getVersion()).toString());
            break;
        case S3:
            S3Configuration s3Configuration = (S3Configuration) ingestion.getProviderConfiguration();
            String bucket = s3Configuration.getBucket();
            S3Destination destination = new S3Destination();
            destination.setSourceName(file);
            destination.setSourceVersion(version);
            if (StringUtils.isNotBlank(bucket) && !defaultBucket.equalsIgnoreCase(bucket)) {
                destination.setS3Bucket(bucket);
            }
            if (s3Configuration.isUpdateCurrentVersion()) {
                destination.setUpdateCurrentVersion(true);
            }
            String destStr = JsonUtils.serialize(destination);
            if (destStr.length() > 1000) {
                throw new IllegalArgumentException("Serialized destination config is longer than 1000 chars: " + destStr);
            }
            progress.setDestination(destStr);
            progress.setSource(file);
            progress.setVersion(version);
            break;
        case BW_RAW:
            BWRawDestination dest = new BWRawDestination();
            dest.setPath("/bwtest/test/test/test");
            String destSer = JsonUtils.serialize(dest);
            progress.setDestination(destSer);
            progress.setSource(file);
            progress.setVersion(version);
            break;
        case PATCH_BOOK:
            // For PATCH_BOOK, version from request is actually datacloud
            // version
            progress.setDataCloudVersion(version);
            progress.setSource(PatchBook.TABLE_NAME);
            progress.setVersion(HdfsPathBuilder.dateFormat.format(new Date()));
            progress.setDestination(ingestionDir.append(progress.getVersion()).toString());
            break;
        default:
            throw new UnsupportedOperationException(
                    String.format("Ingestion type %s is not supported", ingestion.getIngestionType()));
        }
    }

    @Override
    public List<IngestionProgress> getNewIngestionProgresses() {
        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put("Status", ProgressStatus.NEW);
        return getProgressesByField(fields, null);
    }

    @Override
    public List<IngestionProgress> getRetryFailedProgresses() {
        return ingestionProgressEntityMgr.findRetryFailedProgresses();
    }

    @Override
    public List<IngestionProgress> getProcessingProgresses() {
        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put("Status", ProgressStatus.PROCESSING);
        return getProgressesByField(fields, null);
    }

    @Override
    public CamelRouteConfiguration createCamelRouteConfiguration(IngestionProgress progress) {
        switch (progress.getIngestion().getIngestionType()) {
            case SFTP:
            return createSftpToHdfsRouteConfiguration(progress);
        default:
            throw new UnsupportedOperationException(
                    String.format("Ingestion type %s is not supported", progress.getIngestion().getIngestionType()));
        }
    }

    private SftpToHdfsRouteConfiguration createSftpToHdfsRouteConfiguration(
            IngestionProgress progress) {
        SftpToHdfsRouteConfiguration config = new SftpToHdfsRouteConfiguration();
        SftpConfiguration sftpConfig = (SftpConfiguration) progress.getIngestion()
                .getProviderConfiguration();
        Path sourcePath = new Path(progress.getSource());
        Path destPath = new Path(progress.getDestination());
        config.setFileName(sourcePath.getName());
        config.setSftpDir(sourcePath.getParent().toString());
        config.setSftpHost(sftpConfig.getSftpHost());
        config.setSftpPort(sftpConfig.getSftpPort());
        config.setHdfsDir(destPath.getParent().toString());
        config.setSftpUserName(sftpConfig.getSftpUserName());
        config.setSftpPasswordEncrypted(sftpConfig.getSftpPasswordEncrypted());
        return config;
    }

    @Override
    public IngestionProgress saveProgress(IngestionProgress progress) {
        return ingestionProgressEntityMgr.saveProgress(progress);
    }

    @Override
    public void saveProgresses(List<IngestionProgress> progresses) {
        for (IngestionProgress progress : progresses) {
            saveProgress(progress);
        }
    }

    @Override
    public IngestionProgressUpdaterImpl updateProgress(IngestionProgress progress) {
        progress = ingestionProgressEntityMgr.findProgress(progress);
        return new IngestionProgressUpdaterImpl(progress);
    }

    @Override
    public IngestionProgress updateSubmittedProgress(IngestionProgress progress,
            String applicationId) {
        progress.setApplicationId(applicationId);
        progress.setLatestStatusUpdate(new Date());
        progress.setStartTime(new Date());
        if (progress.getStatus() == ProgressStatus.FAILED) {
            progress.setRetries(progress.getRetries() + 1);
        }
        progress.setStatus(ProgressStatus.PROCESSING);
        return ingestionProgressEntityMgr.saveProgress(progress);
    }

    @Override
    public IngestionProgress updateInvalidProgress(IngestionProgress progress, String message) {
        return new IngestionProgressUpdaterImpl(progress).status(ProgressStatus.FAILED)
                .errorMessage(message).commit(false);
    }

    @Override
    public void deleteProgress(IngestionProgress progress) {
        ingestionProgressEntityMgr.deleteProgress(progress);
    }

    @Override
    public void deleteProgressByField(Map<String, Object> fields) {
        ingestionProgressEntityMgr.deleteProgressByField(fields);
    }

    public class IngestionProgressUpdaterImpl implements IngestionProgressUpdater {
        private final IngestionProgress progress;

        IngestionProgressUpdaterImpl(IngestionProgress progress) {
            this.progress = progress;
        }

        public IngestionProgressUpdaterImpl status(ProgressStatus status) {
            this.progress.setStatus(status);
            return this;
        }

        public IngestionProgressUpdaterImpl errorMessage(String errorMessage) {
            this.progress.setErrorMessage(errorMessage);
            return this;
        }

        public IngestionProgressUpdaterImpl size(Long size) {
            this.progress.setSize(size);
            return this;
        }

        public IngestionProgressUpdaterImpl retries(int retries) {
            this.progress.setRetries(retries);
            return this;
        }

        public IngestionProgress commit(boolean persistent) {
            progress.setLatestStatusUpdate(new Date());
            if (persistent) {
                return ingestionProgressEntityMgr.saveProgress(progress);
            } else {
                return progress;
            }
        }
    }
}
