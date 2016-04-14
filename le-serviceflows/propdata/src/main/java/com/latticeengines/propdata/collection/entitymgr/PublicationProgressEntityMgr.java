package com.latticeengines.propdata.collection.entitymgr;

import com.latticeengines.domain.exposed.propdata.manage.Publication;
import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;
import com.latticeengines.domain.exposed.propdata.publication.PublicationDestination;

public interface PublicationProgressEntityMgr {

    PublicationProgress findBySourceVersionUnderMaximumRetry(Publication publication, String sourceVersion);

    PublicationProgress startNewProgress(Publication publication, PublicationDestination destination,
            String sourceVersion, String creator);

    PublicationProgress updateProgress(PublicationProgress progress);
}
