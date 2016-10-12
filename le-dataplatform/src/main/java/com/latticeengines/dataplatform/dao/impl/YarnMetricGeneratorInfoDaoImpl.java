package com.latticeengines.dataplatform.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.YarnMetricGeneratorInfoDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.dataplatform.metrics.YarnMetricGeneratorInfo;

@Component("yarnMetricGeneratorInfoDao")
public class YarnMetricGeneratorInfoDaoImpl extends BaseDaoImpl<YarnMetricGeneratorInfo> implements
        YarnMetricGeneratorInfoDao {

    public YarnMetricGeneratorInfoDaoImpl() {
        super();
    }

    @Override
    protected Class<YarnMetricGeneratorInfo> getEntityClass() {
        return YarnMetricGeneratorInfo.class;
    }

    @Override
    public YarnMetricGeneratorInfo findByName(String name) {
        return findByField("name", name);
    }
}
