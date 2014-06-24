package com.latticeengines.dataplatform.dao.impl;

import com.latticeengines.dataplatform.dao.ModelCommandLogDao;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandLog;

public class ModelCommandLogDaoImpl extends BaseDaoImpl<ModelCommandLog> implements ModelCommandLogDao {
    
    public ModelCommandLogDaoImpl() {
        super();
    }

    @Override
    protected Class<ModelCommandLog> getEntityClass() {
        return ModelCommandLog.class;
    }

}