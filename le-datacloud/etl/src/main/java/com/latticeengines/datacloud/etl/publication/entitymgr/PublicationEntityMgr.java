package com.latticeengines.datacloud.etl.publication.entitymgr;

import java.util.List;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;

public interface PublicationEntityMgr {

    Publication findByPublicationName(@NotNull String publicationName);

    Publication addPublication(@NotNull Publication publication);

    void removePublication(@NotNull String publicationName);

    List<Publication> findAll();
}
