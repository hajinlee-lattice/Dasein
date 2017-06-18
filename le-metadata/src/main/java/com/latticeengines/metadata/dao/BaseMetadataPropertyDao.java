package com.latticeengines.metadata.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.MetadataProperty;

import java.util.List;

public interface BaseMetadataPropertyDao<T extends MetadataProperty<O>, O> extends BaseDao<T> {

    List<T> getPropertiesBelongTo(O Owner);

}
