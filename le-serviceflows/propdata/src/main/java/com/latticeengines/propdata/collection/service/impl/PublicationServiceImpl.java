package com.latticeengines.propdata.collection.service.impl;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
    public PublicationProgress startNewProgressIfAppropriate(Source source, String creator) {
        String currentVersion = hdfsSourceEntityMgr.getCurrentVersion(source);
        Publication publication = publicationEntityMgr.findBySourceName(source.getSourceName());

        PublicationProgress existingProgress = progressEntityMgr.findBySourceVersionUnderMaximumRetry(publication,
                currentVersion);
        if (existingProgress != null) {
            log.info("There is already a progress for current version " + existingProgress);
            return null;
        }

        PublicationDestination destination = constructDestination(publication, source, currentVersion);

        return progressEntityMgr.startNewProgress(publication, destination, currentVersion, creator);
    }

    @Override
    public PublicationProgressUpdaterImpl update(PublicationProgress progress) {
        return new PublicationProgressUpdaterImpl(progress);
    }

    private PublicationDestination constructDestination(Publication publication, Source source, String version) {
        try {
            switch (publication.getPublicationType()) {
            case SQL:
                return constructSqlDestination(publication, source, version);
            default:
                throw new UnsupportedOperationException(
                        "Unknown publication type: " + publication.getPublicationType());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to construct destination for publication "
                    + publication.getPublicationName() + ", source " + source.getSourceName() + ", version " + version,
                    e);
        }
    }

    private SqlDestination constructSqlDestination(Publication publication, Source source, String version)
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
        if (configuration.isVersioned()) {
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

        public PublicationProgressUpdaterImpl incrementRetry() {
            Integer currentRetry = progress.getRetries();
            if (currentRetry == null) {
                currentRetry = 0;
            }
            progress.setRetries(currentRetry + 1);
            return this;
        }

        public PublicationProgress commit() {
            progress.setLatestStatusUpdate(new Date());
            return progressEntityMgr.updateProgress(progress);
        }

    }

}
