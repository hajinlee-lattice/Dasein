package com.latticeengines.propdata.collection.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.propdata.manage.Publication;

public interface PublicationDao extends BaseDao<Publication> {

    List<Publication> findAllForSource(String sourceName);
}
