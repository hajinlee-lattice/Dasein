package com.latticeengines.propdata.engine.publication.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.ProgressStatus;
import com.latticeengines.domain.exposed.propdata.manage.Publication;
import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;
import com.latticeengines.domain.exposed.propdata.publication.PublicationDestination;
import com.latticeengines.domain.exposed.propdata.publication.PublishToSqlConfiguration;
import com.latticeengines.domain.exposed.propdata.publication.SqlDestination;
import com.latticeengines.propdata.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.propdata.core.service.SourceService;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.publication.entitymgr.PublicationEntityMgr;
import com.latticeengines.propdata.engine.publication.entitymgr.PublicationProgressEntityMgr;
import com.latticeengines.propdata.engine.publication.service.PublicationNewProgressValidator;
import com.latticeengines.propdata.engine.publication.service.PublicationProgressService;
import com.latticeengines.propdata.engine.publication.service.PublicationProgressUpdater;

@Component("publicationProgressService")
public class PublicationProgressServiceImpl implements PublicationProgressService {

    private static Log log = LogFactory.getLog(PublicationProgressServiceImpl.class);

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

    @Override
    public PublicationProgress kickoffNewProgress(Publication publication, String creator) {
        String sourceName = publication.getSourceName();
        Source source = sourceService.findBySourceName(sourceName);
        String currentVersion = hdfsSourceEntityMgr.getCurrentVersion(source);
        if (newProgressValidator.isValidToStartNewProgress(publication, currentVersion)) {
            return publishVersion(publication, currentVersion, creator);
        } else {
            return null;
        }
    }

    @Override
    public PublicationProgress publishVersion(Publication publication, String version, String creator) {
        PublicationProgress existingProgress = progressEntityMgr.findBySourceVersionUnderMaximumRetry(publication,
                version);
        if (existingProgress != null) {
            return null;
        }
        PublicationDestination destination = constructDestination(publication, version);
        PublicationProgress progress = progressEntityMgr.startNewProgress(publication, destination, version, creator);
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
        for (Publication publication: publicationEntityMgr.findAll()) {
            PublicationProgress progress = progressEntityMgr.findLatestNonTerminalProgress(publication);
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
                    + publication.getPublicationName() + ", version " + version,
                    e);
        }
    }

    private SqlDestination constructSqlDestination(Publication publication, String version)
            throws Exception {
        PublishToSqlConfiguration configuration = (PublishToSqlConfiguration) publication.getDestinationConfiguration();
        if (configuration == null) {
            throw new IllegalArgumentException(
                    "Publication " + publication.getPublicationName() + " does not have a destination configuration.");
        }
        String tableName = configuration.getDefaultTableName();
        if (tableName == null) {
            throw new IllegalArgumentException("PublishToSqlConfiguration for publication "
                    + publication.getPublicationName() + " does not have a default table anme.");
        }
        if (PublishToSqlConfiguration.PublicationStrategy.VERSIONED.equals(configuration.getPublicationStrategy())) {
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

        public PublicationProgressUpdaterImpl status(ProgressStatus status) {
            this.progress.setStatus(status);
            return this;
        }

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

        public PublicationProgressUpdaterImpl applicationId(ApplicationId applicationId) {
            progress.setApplicationId(applicationId.toString());
            return this;
        }

        public PublicationProgressUpdaterImpl progress(Float progress) {
            this.progress.setProgress(progress);
            return this;
        }

        public PublicationProgressUpdaterImpl fail(String errorMessage) {
            progress.setErrorMessage(errorMessage.substring(0, Math.min(1000, errorMessage.length())));
            progress.setStatus(ProgressStatus.FAILED);
            return this;
        }

        public PublicationProgressUpdater rowsPublished(Long rows) {
            progress.setRowsPublished(rows);
            return this;
        }

        public PublicationProgress commit() {
            progress.setLatestStatusUpdate(new Date());
            return progressEntityMgr.updateProgress(progress);
        }

    }

}
