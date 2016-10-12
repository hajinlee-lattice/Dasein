package com.latticeengines.dataplatform.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.dataplatform.metrics.YarnMetricGeneratorInfo;

public interface YarnMetricGeneratorInfoDao extends BaseDao<YarnMetricGeneratorInfo> {

    YarnMetricGeneratorInfo findByName(String name);
}
