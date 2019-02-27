package com.latticeengines.metadata.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.MetadataProperty;

public interface BaseMetadataPropertyDao<T extends MetadataProperty<O>, O> extends BaseDao<T> {

    List<T> getPropertiesBelongTo(O Owner);

}
