package com.latticeengines.propdata.collection.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.Publication;
import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;
import com.latticeengines.domain.exposed.propdata.publication.PublicationDestination;
import com.latticeengines.domain.exposed.propdata.publication.PublishToSqlConfiguration;
import com.latticeengines.domain.exposed.propdata.publication.SqlDestination;
import com.latticeengines.propdata.collection.entitymgr.PublicationEntityMgr;
import com.latticeengines.propdata.collection.entitymgr.PublicationProgressEntityMgr;
import com.latticeengines.propdata.collection.service.PublicationProgressUpdater;
import com.latticeengines.propdata.collection.service.PublicationService;
import com.latticeengines.propdata.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.propdata.core.source.Source;

@Component("publicationService")
public class PublicationServiceImpl implements PublicationService {

    private static Log log = LogFactory.getLog(PublicationServiceImpl.class);

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private PublicationEntityMgr publicationEntityMgr;

    @Autowired
    private PublicationProgressEntityMgr progressEntityMgr;

    @Override
    public List<PublicationProgress> publishLatest(Source source, String creator) {
        String currentVersion = hdfsSourceEntityMgr.getCurrentVersion(source);
        List<PublicationProgress> progresses = new ArrayList<>();
        for (Publication publication: publicationEntityMgr.findBySourceName(source.getSourceName())) {
            PublicationProgress progress = publishVersion(publication, currentVersion, creator);
            if (progress != null) {
                progresses.add(progress);
            }
        }
        return progresses;
    }

    @Override
    public PublicationProgress publishVersion(Publication publication, String version, String creator) {
        PublicationProgress existingProgress = progressEntityMgr.findBySourceVersionUnderMaximumRetry(publication,
                version);
        if (existingProgress != null) {
            log.info("There is already a progress for version " + existingProgress);
            return null;
        }

        PublicationDestination destination = constructDestination(publication, version);

        return progressEntityMgr.startNewProgress(publication, destination, version, creator);
    }

    @Override
    public PublicationProgressUpdaterImpl update(PublicationProgress progress) {
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

        public PublicationProgressUpdaterImpl status(PublicationProgress.Status status) {
            this.progress.setStatus(status);
            return this;
        }

        public PublicationProgressUpdaterImpl retry() {
            Integer currentRetry = progress.getRetries();
            if (currentRetry == null) {
                currentRetry = 0;
            }
            progress.setRetries(currentRetry + 1);
            progress.setStatus(PublicationProgress.Status.NEW);
            progress.setErrorMessage(null);
            progress.setProgress(0f);
            return this;
        }

        public PublicationProgressUpdaterImpl applicationId(ApplicationId applicationId) {
            progress.setApplicationId(applicationId);
            return this;
        }

        public PublicationProgressUpdaterImpl progress(Float progress) {
            this.progress.setProgress(progress);
            return this;
        }

        public PublicationProgressUpdaterImpl fail(String errorMessage) {
            progress.setErrorMessage(errorMessage);
            progress.setStatus(PublicationProgress.Status.FAILED);
            return this;
        }

        public PublicationProgress commit() {
            progress.setLatestStatusUpdate(new Date());
            return progressEntityMgr.updateProgress(progress);
        }

    }

}
