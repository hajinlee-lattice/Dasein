package com.latticeengines.dataplatform.dao.impl;

import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dataplatform.dao.ModelCommandLogDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandLog;

public class ModelCommandLogDaoImpl extends BaseDaoImpl<ModelCommandLog> implements ModelCommandLogDao {

    public ModelCommandLogDaoImpl() {
        super();
    }

    @Override
    protected Class<ModelCommandLog> getEntityClass() {
        return ModelCommandLog.class;
    }

    @SuppressWarnings({ "unchecked", "deprecation" })
    @Override
    @Transactional(value = "dlorchestration", propagation = Propagation.REQUIRED)
    public List<ModelCommandLog> findByModelCommand(ModelCommand modelCommand) {
        Session session = getSessionFactory().getCurrentSession();
        List<ModelCommandLog> logs = session
                .createCriteria(ModelCommandLog.class)
                .add(Restrictions.eq("modelCommand", modelCommand))
                .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY).list();

        return logs;
    }

}
