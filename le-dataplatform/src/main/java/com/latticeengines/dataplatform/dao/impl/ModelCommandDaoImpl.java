package com.latticeengines.dataplatform.dao.impl;

import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;

import com.latticeengines.dataplatform.dao.ModelCommandDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatus;

public class ModelCommandDaoImpl extends BaseDaoImpl<ModelCommand> implements ModelCommandDao {

    public ModelCommandDaoImpl() {
        super();
    }

    @Override
    protected Class<ModelCommand> getEntityClass() {
        return ModelCommand.class;
    }

    @SuppressWarnings({ "unchecked", "deprecation" })
    @Override
    public List<ModelCommand> getNewAndInProgress() {
        Session session = getSessionFactory().getCurrentSession();
        List<ModelCommand> commands = session
                .createCriteria(ModelCommand.class)
                .add(Restrictions.eq("modelId", ModelCommand.TAHOE))
                .add(Restrictions.or(Restrictions.eq("commandStatus", ModelCommandStatus.NEW),
                        Restrictions.eq("commandStatus", ModelCommandStatus.IN_PROGRESS)))
                .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY).list();

        return commands;
    }

}
