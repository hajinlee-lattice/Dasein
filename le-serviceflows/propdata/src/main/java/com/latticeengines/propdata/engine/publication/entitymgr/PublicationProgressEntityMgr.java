package com.latticeengines.propdata.engine.publication.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.Publication;
import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;
import com.latticeengines.domain.exposed.propdata.publication.PublicationDestination;

public interface PublicationProgressEntityMgr {

    PublicationProgress findBySourceVersionUnderMaximumRetry(Publication publication, String sourceVersion);

    PublicationProgress findLatestUnderMaximumRetry(Publication publication);

    PublicationProgress startNewProgress(Publication publication, PublicationDestination destination,
            String sourceVersion, String creator);

    PublicationProgress updateProgress(PublicationProgress progress);

    PublicationProgress findLatestNonTerminalProgress(Publication publication);

    List<PublicationProgress> findAllForPublication(Publication publication);

    PublicationProgress findByPid(Long pid);

}
