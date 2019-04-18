package com.latticeengines.dataplatform.dao.impl;

import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;

import com.latticeengines.dataplatform.dao.ModelCommandStateDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandState;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;

public class ModelCommandStateDaoImpl extends BaseDaoImpl<ModelCommandState> implements ModelCommandStateDao {

    public ModelCommandStateDaoImpl() {
        super();
    }

    @Override
    protected Class<ModelCommandState> getEntityClass() {
        return ModelCommandState.class;
    }

    @SuppressWarnings({ "unchecked", "deprecation" })
    @Override
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
