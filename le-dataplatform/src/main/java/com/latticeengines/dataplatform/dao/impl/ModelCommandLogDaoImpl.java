package com.latticeengines.dataplatform.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.ModelCommandLogDao;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandLog;

@Component("modelCommandLogDao")
public class ModelCommandLogDaoImpl extends BaseDaoImpl<ModelCommandLog> implements ModelCommandLogDao {

    public ModelCommandLogDaoImpl() {
        super();
    }

    @Override
    public ModelCommandLog deserialize(String id, String content) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected Class<ModelCommandLog> getEntityClass() {
        return ModelCommandLog.class;
    }

}