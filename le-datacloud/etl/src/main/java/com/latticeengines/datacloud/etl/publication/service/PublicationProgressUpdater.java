package com.latticeengines.datacloud.etl.publication.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationDestination;

public interface PublicationProgressUpdater {

    PublicationProgressUpdater status(ProgressStatus status);
    PublicationProgressUpdater fail(String errorMessage);
    PublicationProgressUpdater retry();
    PublicationProgressUpdater applicationId(ApplicationId applicationId);
    PublicationProgressUpdater progress(Float progress);
    PublicationProgressUpdater rowsPublished(Long rows);
    PublicationProgressUpdater destination(PublicationDestination destination);
    PublicationProgress commit();

}
