package com.latticeengines.datacloud.etl.publication.dao;

import java.util.List;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;

public interface PublicationDao extends BaseDao<Publication> {

    List<Publication> findAllForSource(@NotNull String sourceName);

}
