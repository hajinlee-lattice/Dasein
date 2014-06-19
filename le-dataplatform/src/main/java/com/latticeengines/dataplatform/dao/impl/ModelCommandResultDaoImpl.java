package com.latticeengines.dataplatform.dao.impl;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Restrictions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.ModelCommandResultDao;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandResult;

@Component("modelCommandResultDao")
public class ModelCommandResultDaoImpl extends BaseDaoImpl<ModelCommandResult> implements ModelCommandResultDao {

    @Autowired
    protected SessionFactory sessionFactoryDlOrchestration;

    @Override
    protected SessionFactory getSessionFactory() {        
        return sessionFactoryDlOrchestration;
    }
    
    public ModelCommandResultDaoImpl() {
        super();
    }

    @Override
    protected Class<ModelCommandResult> getEntityClass() {
        return ModelCommandResult.class;
    }
    
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