package com.latticeengines.modelquality.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelquality.Algorithm;
import com.latticeengines.modelquality.dao.AlgorithmDao;

@Component("qualityAlgorithmDao")
public class AlgorithmDaoImpl extends BaseDaoImpl<Algorithm> implements AlgorithmDao {

    @Override
    protected Class<Algorithm> getEntityClass() {
        return Algorithm.class;
    }

}
