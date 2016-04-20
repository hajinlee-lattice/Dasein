package com.latticeengines.propdata.engine.publication.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.propdata.manage.ProgressStatus;
import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;

public interface PublicationProgressUpdater {

    PublicationProgressUpdater status(ProgressStatus status);
    PublicationProgressUpdater fail(String errorMessage);
    PublicationProgressUpdater retry();
    PublicationProgressUpdater applicationId(ApplicationId applicationId);
    PublicationProgressUpdater progress(Float progress);
    PublicationProgressUpdater rowsPublished(Long rows);
    PublicationProgress commit();

}
