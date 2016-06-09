package com.latticeengines.propdata.engine.ingestion.service.impl;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.route.CamelRouteConfiguration;
import com.latticeengines.domain.exposed.eai.route.SftpToHdfsRouteConfiguration;
import com.latticeengines.domain.exposed.propdata.ingestion.SftpConfiguration;
import com.latticeengines.domain.exposed.propdata.manage.Ingestion;
import com.latticeengines.domain.exposed.propdata.manage.IngestionProgress;
import com.latticeengines.domain.exposed.propdata.manage.ProgressStatus;
import com.latticeengines.propdata.core.IngestionNames;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.service.impl.HdfsPodContext;
import com.latticeengines.propdata.engine.ingestion.entitymgr.IngestionProgressEntityMgr;
import com.latticeengines.propdata.engine.ingestion.service.IngestionProgressService;
import com.latticeengines.propdata.engine.ingestion.service.IngestionProgressUpdater;

@Component("ingestionProgressService")
public class IngestionProgressServiceImpl implements IngestionProgressService {

    @Autowired
    IngestionProgressEntityMgr ingestionProgressEntityMgr;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Override
    public List<IngestionProgress> getProgressesByField(Map<String, Object> fields) {
        return ingestionProgressEntityMgr.getProgressesByField(fields);
    }

    @Override
    public IngestionProgress createPreprocessProgress(Ingestion ingestion, String triggeredBy,
            String file) {
        String source = constructSource(ingestion, file);
        String destination = constructDestination(ingestion, file);
        IngestionProgress progress = createProgress(ingestion, source, destination, triggeredBy);
        return progress;
    }

    public IngestionProgress createProgress(Ingestion ingestion, String source, String destination,
            String triggeredBy) {
        IngestionProgress progress = new IngestionProgress();
        progress.setDestination(destination);
        progress.setHdfsPod(HdfsPodContext.getHdfsPodId());
        progress.setIngestion(ingestion);
        progress.setLatestStatusUpdateTime(new Date());
        progress.setRetries(0);
        progress.setSource(source);
        progress.setStatus(ProgressStatus.NEW);
        progress.setTriggeredBy(triggeredBy);
        return progress;
    }

    @Override
    public String constructSource(Ingestion ingestion, String fileName) {
        switch (ingestion.getIngestionType()) {
        case SFTP_TO_HDFS:
            SftpConfiguration sftpConfiguration = (SftpConfiguration) ingestion
                    .getProviderConfiguration();
            Path fileSource = new Path(sftpConfiguration.getSftpDir(), fileName);
            return fileSource.toString();
        default:
            throw new UnsupportedOperationException(
                    "Ingestion type " + ingestion.getIngestionType() + " is not supported.");
        }
    }

    @Override
    public String constructDestination(Ingestion ingestion, String fileName) {
        com.latticeengines.domain.exposed.camille.Path fileDest = hdfsPathBuilder
                .constructIngestionDir(ingestion.getIngestionName());
        if (ingestion.getIngestionName().equals(IngestionNames.BOMBORA_FIREHOSE)) {
            try {
                DateFormat format = new SimpleDateFormat("yyyyMMdd");
                TimeZone timezone = TimeZone.getTimeZone("UTC");
                format.setTimeZone(timezone);
                String fileVersion = HdfsPathBuilder.dateFormat
                        .format(format.parse(fileName.substring(17, 25)));
                fileDest = fileDest.append(fileVersion);
            } catch (ParseException e) {
                throw new RuntimeException("Failed to parse source file version");
            }

        } else {
            String fileVersion = HdfsPathBuilder.dateFormat.format(new Date());
            fileDest = fileDest.append(fileVersion);
        }
        fileDest = fileDest.append(fileName);
        return fileDest.toString();
    }

    @Override
    public List<IngestionProgress> getNewIngestionProgresses() {
        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put("Status", ProgressStatus.NEW);
        return getProgressesByField(fields);
    }

    @Override
    public List<IngestionProgress> getRetryFailedProgresses() {
        return ingestionProgressEntityMgr.getRetryFailedProgresses();
    }

    @Override
    public CamelRouteConfiguration createCamelRouteConfiguration(IngestionProgress progress) {
        switch (progress.getIngestion().getIngestionType()) {
        case SFTP_TO_HDFS:
            return createSftpToHdfsRouteConfiguration(progress);
        default:
            throw new UnsupportedOperationException("Ingestion type "
                    + progress.getIngestion().getIngestionType() + " is not supported.");
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
        progress = ingestionProgressEntityMgr.getProgress(progress);
        return new IngestionProgressUpdaterImpl(progress);
    }

    @Override
    public IngestionProgress updateSubmittedProgress(IngestionProgress progress,
            String applicationId) {
        progress.setApplicationId(applicationId);
        progress.setLatestStatusUpdateTime(new Date());
        progress.setStartTime(new Date());
        if (progress.getStatus() == ProgressStatus.FAILED) {
            progress.setRetries(progress.getRetries() + 1);
        }
        progress.setStatus(ProgressStatus.PROCESSING);
        ingestionProgressEntityMgr.saveProgress(progress);
        return progress;
    }

    @Override
    public IngestionProgress updateDuplicateProgress(IngestionProgress progress) {
        return new IngestionProgressUpdaterImpl(progress).status(ProgressStatus.FAILED)
                .errorMessage("There is already a progress ingesting " + progress.getSource()
                        + " to " + progress.getDestination())
                .commit(false);
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

        public IngestionProgress commit(boolean persistent) {
            progress.setLatestStatusUpdateTime(new Date());
            if (persistent) {
                return ingestionProgressEntityMgr.saveProgress(progress);
            } else {
                return progress;
            }
        }
    }
}
