package com.latticeengines.propdata.collection.service;

import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;

public interface PublicationProgressUpdater {

    PublicationProgressUpdater status(PublicationProgress.Status status);
    PublicationProgressUpdater incrementRetry();
    PublicationProgress commit();

}
