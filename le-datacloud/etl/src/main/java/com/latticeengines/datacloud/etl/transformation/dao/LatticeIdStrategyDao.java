package com.latticeengines.datacloud.etl.transformation.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datacloud.manage.LatticeIdStrategy;

public interface LatticeIdStrategyDao extends BaseDao<LatticeIdStrategy> {
    List<LatticeIdStrategy> getStrategyByName(String name);
}
