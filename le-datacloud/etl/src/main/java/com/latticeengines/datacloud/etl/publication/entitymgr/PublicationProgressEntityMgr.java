package com.latticeengines.datacloud.etl.publication.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationDestination;

public interface PublicationProgressEntityMgr {

    PublicationProgress findBySourceVersionUnderMaximumRetry(Publication publication, String sourceVersion);

    PublicationProgress findLatestUnderMaximumRetry(Publication publication);

    PublicationProgress startNewProgress(Publication publication, PublicationDestination destination,
                                         String sourceVersion, String creator);

    PublicationProgress runNewProgress(Publication publication, PublicationDestination destination,
                                       String sourceVersion, String creator);

    PublicationProgress updateProgress(PublicationProgress progress);

    PublicationProgress findLatestNonTerminalProgress(Publication publication);

    List<PublicationProgress> findAllForPublication(Publication publication);

    PublicationProgress findByPid(Long pid);

    List<PublicationProgress> findStatusByPublicationVersion(Publication publication, String version);

}
