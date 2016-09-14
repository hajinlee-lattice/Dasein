package com.latticeengines.modelquality.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.modelquality.Sampling;
import com.latticeengines.domain.exposed.modelquality.SamplingPropertyDef;
import com.latticeengines.domain.exposed.modelquality.SamplingPropertyValue;
import com.latticeengines.modelquality.dao.SamplingDao;
import com.latticeengines.modelquality.dao.SamplingPropertyDefDao;
import com.latticeengines.modelquality.dao.SamplingPropertyValueDao;
import com.latticeengines.modelquality.entitymgr.SamplingEntityMgr;

@Component("qualitySamplingEntityMgr")
public class SamplingEntityMgrImpl extends BaseEntityMgrImpl<Sampling> implements SamplingEntityMgr {
    
    @Autowired
    private SamplingDao samplingDao;
    
    @Autowired
    private SamplingPropertyDefDao samplingPropertyDefDao;
    
    @Autowired
    private SamplingPropertyValueDao samplingPropertyValueDao;

    @Override
    public BaseDao<Sampling> getDao() {
        return samplingDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(Sampling sampling) {
        samplingDao.create(sampling);
        
        for (SamplingPropertyDef propertyDef : sampling.getSamplingPropertyDefs()) {
            samplingPropertyDefDao.create(propertyDef);
            
            for (SamplingPropertyValue propertyValue : propertyDef.getSamplingPropertyValues()) {
                samplingPropertyValueDao.create(propertyValue);
            }
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Sampling findByName(String samplingConfigName) {
        return samplingDao.findByField("NAME", samplingConfigName);
    }

}
