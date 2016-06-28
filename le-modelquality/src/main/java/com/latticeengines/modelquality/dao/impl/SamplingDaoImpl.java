package com.latticeengines.modelquality.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelquality.Sampling;
import com.latticeengines.modelquality.dao.SamplingDao;

@Component("qualitySamplingDao")
public class SamplingDaoImpl extends BaseDaoImpl<Sampling> implements SamplingDao {

    @Override
    protected Class<Sampling> getEntityClass() {
        return Sampling.class;
    }

}
