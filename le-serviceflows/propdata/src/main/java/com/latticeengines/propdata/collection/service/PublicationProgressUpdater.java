package com.latticeengines.propdata.collection.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;

public interface PublicationProgressUpdater {

    PublicationProgressUpdater status(PublicationProgress.Status status);
    PublicationProgressUpdater retry();
    PublicationProgressUpdater applicationId(ApplicationId applicationId);
    PublicationProgressUpdater progress(Float progress);
    PublicationProgress commit();

}
