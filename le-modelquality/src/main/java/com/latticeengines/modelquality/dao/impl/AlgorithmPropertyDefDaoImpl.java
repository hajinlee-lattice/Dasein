package com.latticeengines.modelquality.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelquality.AlgorithmPropertyDef;
import com.latticeengines.modelquality.dao.AlgorithmPropertyDefDao;

@Component("algorithmPropertyDefDao")
public class AlgorithmPropertyDefDaoImpl extends ModelQualityBaseDaoImpl<AlgorithmPropertyDef> implements AlgorithmPropertyDefDao {

    @Override
    protected Class<AlgorithmPropertyDef> getEntityClass() {
        return AlgorithmPropertyDef.class;
    }

}
