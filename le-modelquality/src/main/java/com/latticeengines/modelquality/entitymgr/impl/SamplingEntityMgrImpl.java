package com.latticeengines.modelquality.entitymgr.impl;

import javax.inject.Inject;

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

    @Inject
    private SamplingDao samplingDao;

    @Inject
    private SamplingPropertyDefDao samplingPropertyDefDao;

    @Inject
    private SamplingPropertyValueDao samplingPropertyValueDao;

    @Override
    public BaseDao<Sampling> getDao() {
        return samplingDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(Sampling sampling) {
        sampling.setName(sampling.getName().replace('/', '_'));
        samplingDao.create(sampling);

        for (SamplingPropertyDef propertyDef : sampling.getSamplingPropertyDefs()) {
            samplingPropertyDefDao.create(propertyDef);

            for (SamplingPropertyValue propertyValue : propertyDef.getSamplingPropertyValues()) {
                samplingPropertyValueDao.create(propertyValue);
            }
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Sampling findByName(String samplingConfigName) {
        return samplingDao.findByField("NAME", samplingConfigName);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Sampling getLatestProductionVersion() {
        return samplingDao.findByMaxVersion();
    }
}
