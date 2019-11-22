package com.latticeengines.datacloud.etl.publication.entitymgr;

import java.util.List;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationDestination;

public interface PublicationProgressEntityMgr {

    PublicationProgress findBySourceVersionUnderMaximumRetry(@NotNull Publication publication,
            @NotNull String sourceVersion);

    PublicationProgress findLatestUnderMaximumRetry(@NotNull Publication publication);

    PublicationProgress startNewProgress(@NotNull Publication publication, @NotNull PublicationDestination destination,
            @NotNull String sourceVersion, @NotNull String creator);

    PublicationProgress runNewProgress(@NotNull Publication publication, @NotNull PublicationDestination destination,
            @NotNull String sourceVersion, @NotNull String creator);

    PublicationProgress updateProgress(@NotNull PublicationProgress progress);

    PublicationProgress findLatestNonTerminalProgress(@NotNull Publication publication);

    List<PublicationProgress> findAllForPublication(@NotNull Publication publication);

    PublicationProgress findByPid(@NotNull Long pid);

    List<PublicationProgress> findStatusByPublicationVersion(@NotNull Publication publication, String version);

    String getLatestSuccessVersion(@NotNull String publicationName);
}
