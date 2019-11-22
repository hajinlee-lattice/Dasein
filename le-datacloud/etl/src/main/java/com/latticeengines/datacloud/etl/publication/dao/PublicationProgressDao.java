package com.latticeengines.datacloud.etl.publication.dao;

import java.util.List;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;

public interface PublicationProgressDao extends BaseDao<PublicationProgress> {

    List<PublicationProgress> findAllForPublication(@NotNull Long publicationId);

    List<PublicationProgress> getStatusForLatestVersion(@NotNull Publication publication, @NotNull String version);

    String getLatestSuccessVersion(@NotNull String publicationName);
}
