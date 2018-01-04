package com.latticeengines.modelquality.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelquality.SamplingPropertyDef;
import com.latticeengines.modelquality.dao.SamplingPropertyDefDao;

@Component("samplingPropertyDefDao")
public class SamplingPropertyDefDaoImpl extends ModelQualityBaseDaoImpl<SamplingPropertyDef> implements SamplingPropertyDefDao {

    @Override
    protected Class<SamplingPropertyDef> getEntityClass() {
        return SamplingPropertyDef.class;
    }

}
