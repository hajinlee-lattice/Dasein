package com.latticeengines.dataplatform.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dataplatform.dao.YarnMetricGeneratorInfoDao;
import com.latticeengines.dataplatform.exposed.entitymanager.YarnMetricGeneratorInfoEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.dataplatform.metrics.YarnMetricGeneratorInfo;

@Component("yarnMetricGeneratorInfoEntityMgr")
public class YarnMetricGeneratorInfoEntityMgrImpl extends BaseEntityMgrImpl<YarnMetricGeneratorInfo> implements
        YarnMetricGeneratorInfoEntityMgr {

    @Autowired
    private YarnMetricGeneratorInfoDao dao;

    public YarnMetricGeneratorInfoEntityMgrImpl() {
        super();
    }

    @Override
    public BaseDao<YarnMetricGeneratorInfo> getDao() {
        return dao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public YarnMetricGeneratorInfo findByName(String name) {
        return dao.findByName(name);
    }
}
