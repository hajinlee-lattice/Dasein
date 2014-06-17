package com.latticeengines.dataplatform.dao.impl;

import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Restrictions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dataplatform.dao.ModelCommandStateDao;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandState;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;

@Component("modelCommandStateDao")
public class ModelCommandStateDaoImpl extends BaseDaoImpl<ModelCommandState> implements ModelCommandStateDao {

    @Autowired
    protected SessionFactory sessionFactoryDlOrchestration;

    @Override
    protected SessionFactory getSessionFactory() {        
        return sessionFactoryDlOrchestration;
    }
    
    public ModelCommandStateDaoImpl() {
        super();
    }

    @Override
    protected Class<ModelCommandState> getEntityClass() {
        return ModelCommandState.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    @Transactional(value="dlorchestration", propagation = Propagation.REQUIRED)
    public List<ModelCommandState> findByModelCommandAndStep(ModelCommand modelCommand,
            ModelCommandStep modelCommandStep) {
        Session session = getSessionFactory().getCurrentSession();
        List<ModelCommandState> states = session.createCriteria(ModelCommandState.class)
                .add(Restrictions.eq("modelCommand", modelCommand))
                .add(Restrictions.eq("modelCommandStep", modelCommandStep))
                .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY).list();
        return states;
    }

}