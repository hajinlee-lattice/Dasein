package com.latticeengines.dataplatform.dao.impl;

import com.latticeengines.dataplatform.dao.ModelCommandParameterDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandParameter;

public class ModelCommandParameterDaoImpl extends BaseDaoImpl<ModelCommandParameter> implements ModelCommandParameterDao {

    public ModelCommandParameterDaoImpl() {
        super();
    }

    @Override
    protected Class<ModelCommandParameter> getEntityClass() {
        return ModelCommandParameter.class;
    }

}
