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

import com.latticeengines.dataplatform.dao.ModelCommandDao;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatus;

@Component("modelCommandDao")
public class ModelCommandDaoImpl extends BaseDaoImpl<ModelCommand> implements ModelCommandDao {

    @Autowired
    protected SessionFactory sessionFactoryDlOrchestration;

    @Override
    protected SessionFactory getSessionFactory() {        
        return sessionFactoryDlOrchestration;
    }
    
    public ModelCommandDaoImpl() {
        super();
    }

    @Override
    protected Class<ModelCommand> getEntityClass() {
        return ModelCommand.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    @Transactional(value="dlorchestration", propagation = Propagation.REQUIRED)
    public List<ModelCommand> getNewAndInProgress() {
        Session session = getSessionFactory().getCurrentSession();
        List<ModelCommand> commands = session
                .createCriteria(ModelCommand.class)
                .add(Restrictions.or(Restrictions.eq("commandStatus", ModelCommandStatus.NEW),
                        Restrictions.eq("commandStatus", ModelCommandStatus.IN_PROGRESS)))
                .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY).list();

        return commands;
    }

}