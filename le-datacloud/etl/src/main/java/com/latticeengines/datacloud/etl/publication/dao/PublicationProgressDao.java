package com.latticeengines.datacloud.etl.publication.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;

public interface PublicationProgressDao extends BaseDao<PublicationProgress> {

    List<PublicationProgress> findAllForPublication(Long publicationId);
}
