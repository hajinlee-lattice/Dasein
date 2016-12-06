package com.latticeengines.datacloud.core.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalDimension;

public interface CategoricalDimensionDao extends BaseDao<CategoricalDimension> {

    CategoricalDimension findBySourceDimension(String source, String dimension);

}
