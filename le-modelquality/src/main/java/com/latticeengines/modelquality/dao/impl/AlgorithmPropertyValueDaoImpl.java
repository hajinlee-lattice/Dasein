package com.latticeengines.modelquality.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelquality.AlgorithmPropertyValue;
import com.latticeengines.modelquality.dao.AlgorithmPropertyValueDao;

@Component("algorithmPropertyValueDao")
public class AlgorithmPropertyValueDaoImpl extends ModelQualityBaseDaoImpl<AlgorithmPropertyValue>
        implements AlgorithmPropertyValueDao {

    @Override
    protected Class<AlgorithmPropertyValue> getEntityClass() {
        return AlgorithmPropertyValue.class;
    }

}
