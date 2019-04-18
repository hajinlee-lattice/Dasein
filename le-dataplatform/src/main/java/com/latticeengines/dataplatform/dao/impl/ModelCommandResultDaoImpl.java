package com.latticeengines.dataplatform.dao.impl;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;

import com.latticeengines.dataplatform.dao.ModelCommandResultDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandResult;

public class ModelCommandResultDaoImpl extends BaseDaoImpl<ModelCommandResult> implements ModelCommandResultDao {


    public ModelCommandResultDaoImpl() {
        super();
    }

    @Override
    protected Class<ModelCommandResult> getEntityClass() {
        return ModelCommandResult.class;
    }

    @SuppressWarnings("deprecation")
    @Override
    public ModelCommandResult findByModelCommand(ModelCommand modelCommand) {
        Session session = getSessionFactory().getCurrentSession();
        Object result = session
                .createCriteria(ModelCommandResult.class)
                .add(Restrictions.eq("commandId", modelCommand.getPid()))
                .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY).uniqueResult();

        ModelCommandResult modelCommandResult = null;
        if (result != null) {
            modelCommandResult = (ModelCommandResult)result;
        }

        return modelCommandResult;

    }

}
