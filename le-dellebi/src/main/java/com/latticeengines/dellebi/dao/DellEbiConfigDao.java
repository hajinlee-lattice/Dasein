package com.latticeengines.dellebi.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.dellebi.DellEbiConfig;

public interface DellEbiConfigDao extends BaseDao<DellEbiConfig> {

    DellEbiConfig getConfig(String type);

    List<DellEbiConfig> queryConfigs();

}
