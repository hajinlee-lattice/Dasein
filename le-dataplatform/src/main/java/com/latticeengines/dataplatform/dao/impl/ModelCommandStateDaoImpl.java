package com.latticeengines.dataplatform.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.ModelCommandStateDao;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandState;

@Component("modelCommandStateDao")
public class ModelCommandStateDaoImpl extends BaseDaoImpl<ModelCommandState> implements ModelCommandStateDao {

    public ModelCommandStateDaoImpl() {
        super();
    }

    @Override
    protected Class<ModelCommandState> getEntityClass() {        
        return ModelCommandState.class;
    }


}