package com.latticeengines.datacloud.etl.publication.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.publication.entitymgr.PublicationEntityMgr;
import com.latticeengines.datacloud.etl.publication.entitymgr.PublicationProgressEntityMgr;
import com.latticeengines.datacloud.etl.publication.service.PublicationNewProgressValidator;
import com.latticeengines.datacloud.etl.publication.service.PublicationProgressService;
import com.latticeengines.datacloud.etl.publication.service.PublicationProgressUpdater;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationDestination;
import com.latticeengines.domain.exposed.datacloud.publication.PublishToSqlConfiguration;
import com.latticeengines.domain.exposed.datacloud.publication.SqlDestination;

import edu.emory.mathcs.backport.java.util.Collections;

@Component("publicationProgressService")
public class PublicationProgressServiceImpl implements PublicationProgressService {

    private static Logger log = LoggerFactory.getLogger(PublicationProgressServiceImpl.class);

    @Autowired
    private PublicationEntityMgr publicationEntityMgr;

    @Autowired
    private PublicationProgressEntityMgr progressEntityMgr;

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private SourceService sourceService;

    @Autowired
    private PublicationNewProgressValidator newProgressValidator;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private Configuration yarnConfiguration;

    @Override
    public PublicationProgress kickoffNewProgress(Publication publication, String creator)
            throws IOException {
        String currentVersion = null;
        switch (publication.getMaterialType()) {
            case SOURCE:
                String sourceName = publication.getSourceName();
                Source source = sourceService.findBySourceName(sourceName);
                currentVersion = hdfsSourceEntityMgr.getCurrentVersion(source);
                break;
            case INGESTION:
                String ingestionName = publication.getSourceName();
                String ingestionDir = hdfsPathBuilder.constructIngestionDir(ingestionName)
                        .toString();
                List<String> versions = HdfsUtils.getFilesForDir(yarnConfiguration, ingestionDir);
                if (versions != null && !versions.isEmpty()) {
                    Path currentVersionPath = new Path((String) Collections.max(versions));
                    currentVersion = currentVersionPath.getName();
                }
                break;
        }
        if (currentVersion != null
                && newProgressValidator.isValidToStartNewProgress(publication, currentVersion)) {
            return publishVersion(publication, currentVersion, creator);
        } else {
            return null;
        }
    }

    @Override
    public PublicationProgress publishVersion(Publication publication, String version,
            String creator) {
        PublicationProgress existingProgress = progressEntityMgr
                .findBySourceVersionUnderMaximumRetry(publication, version);
        if (existingProgress != null) {
            return null;
        }
        PublicationDestination destination = constructDestination(publication, version);
        PublicationProgress progress = progressEntityMgr.startNewProgress(publication, destination,
                version, creator);
        log.info("Kick off new progress [" + progress + "]");
        return progress;
    }

    @Override
    public PublicationProgressUpdaterImpl update(PublicationProgress progress) {
        progress = progressEntityMgr.findByPid(progress.getPid());
        return new PublicationProgressUpdaterImpl(progress);
    }

    @Override
    public List<PublicationProgress> scanNonTerminalProgresses() {
        List<PublicationProgress> progresses = new ArrayList<>();
        for (Publication publication : publicationEntityMgr.findAll()) {
            PublicationProgress progress = progressEntityMgr
                    .findLatestNonTerminalProgress(publication);
            if (progress != null) {
                progresses.add(progress);
            }
        }
        return progresses;
    }

    private PublicationDestination constructDestination(Publication publication, String version) {
        try {
            switch (publication.getPublicationType()) {
                case SQL:
                    return constructSqlDestination(publication, version);
                default:
                    throw new UnsupportedOperationException(
                            "Unknown publication type: " + publication.getPublicationType());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to construct destination for publication "
                    + publication.getPublicationName() + ", version " + version, e);
        }
    }

    private SqlDestination constructSqlDestination(Publication publication, String version)
            throws Exception {
        PublishToSqlConfiguration configuration = (PublishToSqlConfiguration) publication
                .getDestinationConfiguration();
        if (configuration == null) {
            throw new IllegalArgumentException("Publication " + publication.getPublicationName()
                    + " does not have a destination configuration.");
        }
        String tableName = configuration.getDefaultTableName();
        if (tableName == null) {
            throw new IllegalArgumentException("PublishToSqlConfiguration for publication "
                    + publication.getPublicationName() + " does not have a default table anme.");
        }
        if (PublishToSqlConfiguration.PublicationStrategy.VERSIONED
                .equals(configuration.getPublicationStrategy())) {
            tableName += "_" + version;
        }
        SqlDestination destination = new SqlDestination();
        destination.setTableName(tableName);
        return destination;
    }

    public class PublicationProgressUpdaterImpl implements PublicationProgressUpdater {

        private final PublicationProgress progress;

        PublicationProgressUpdaterImpl(PublicationProgress progress) {
            this.progress = progress;
        }

        @Override
        public PublicationProgressUpdaterImpl status(ProgressStatus status) {
            this.progress.setStatus(status);
            return this;
        }

        @Override
        public PublicationProgressUpdaterImpl retry() {
            Integer currentRetry = progress.getRetries();
            if (currentRetry == null) {
                currentRetry = 0;
            }
            progress.setRetries(currentRetry + 1);
            progress.setStatus(ProgressStatus.NEW);
            progress.setErrorMessage(null);
            progress.setProgress(0f);
            return this;
        }

        @Override
        public PublicationProgressUpdaterImpl applicationId(ApplicationId applicationId) {
            progress.setApplicationId(applicationId.toString());
            return this;
        }

        @Override
        public PublicationProgressUpdaterImpl progress(Float progress) {
            this.progress.setProgress(progress);
            return this;
        }

        @Override
        public PublicationProgressUpdaterImpl fail(String errorMessage) {
            progress.setErrorMessage(
                    errorMessage.substring(0, Math.min(1000, errorMessage.length())));
            progress.setStatus(ProgressStatus.FAILED);
            return this;
        }

        @Override
        public PublicationProgressUpdater rowsPublished(Long rows) {
            progress.setRowsPublished(rows);
            return this;
        }

        @Override
        public PublicationProgress commit() {
            progress.setLatestStatusUpdate(new Date());
            return progressEntityMgr.updateProgress(progress);
        }

    }

}
