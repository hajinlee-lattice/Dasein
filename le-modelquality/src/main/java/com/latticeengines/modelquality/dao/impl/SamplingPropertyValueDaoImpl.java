package com.latticeengines.modelquality.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelquality.SamplingPropertyValue;
import com.latticeengines.modelquality.dao.SamplingPropertyValueDao;

@Component("samplingPropertyValueDao")
public class SamplingPropertyValueDaoImpl extends ModelQualityBaseDaoImpl<SamplingPropertyValue>
        implements SamplingPropertyValueDao {

    @Override
    protected Class<SamplingPropertyValue> getEntityClass() {
        return SamplingPropertyValue.class;
    }

}
