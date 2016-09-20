package com.latticeengines.modelquality.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.modelquality.Algorithm;
import com.latticeengines.domain.exposed.modelquality.AlgorithmPropertyDef;
import com.latticeengines.domain.exposed.modelquality.AlgorithmPropertyValue;
import com.latticeengines.modelquality.dao.AlgorithmDao;
import com.latticeengines.modelquality.dao.AlgorithmPropertyDefDao;
import com.latticeengines.modelquality.dao.AlgorithmPropertyValueDao;
import com.latticeengines.modelquality.entitymgr.AlgorithmEntityMgr;

@Component("qualityAlgorithmEntityMgr")
public class AlgorithmEntityMgrImpl extends BaseEntityMgrImpl<Algorithm> implements AlgorithmEntityMgr {

    @Autowired
    private AlgorithmDao algorithmDao;

    @Autowired
    private AlgorithmPropertyDefDao algorithmPropertyDefDao;

    @Autowired
    private AlgorithmPropertyValueDao algorithmPropertyValueDao;

    @Override
    public BaseDao<Algorithm> getDao() {
        return algorithmDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(Algorithm algorithm) {
        algorithmDao.create(algorithm);
        for (AlgorithmPropertyDef propertyDef : algorithm.getAlgorithmPropertyDefs()) {
            algorithmPropertyDefDao.create(propertyDef);

            for (AlgorithmPropertyValue propertyValue : propertyDef.getAlgorithmPropertyValues()) {
                algorithmPropertyValueDao.create(propertyValue);
            }
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Algorithm findByName(String algorithmName) {
        return algorithmDao.findByField("NAME", algorithmName);
    }

}
