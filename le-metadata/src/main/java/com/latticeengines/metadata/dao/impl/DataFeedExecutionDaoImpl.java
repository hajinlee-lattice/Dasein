package com.latticeengines.metadata.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Restrictions;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution.Status;
import com.latticeengines.metadata.dao.DataFeedExecutionDao;

@Component("datafeedExecutionDao")
public class DataFeedExecutionDaoImpl extends BaseDaoImpl<DataFeedExecution> implements DataFeedExecutionDao {

    @Override
    protected Class<DataFeedExecution> getEntityClass() {
        return DataFeedExecution.class;
    }

    @Override
    public DataFeedExecution findConsolidatingExecution(DataFeed datafeed) {
        Session session = getSessionFactory().getCurrentSession();
        Class<DataFeedExecution> entityClz = getEntityClass();
        Object res = session.createCriteria(entityClz) //
                .addOrder(Order.desc("pid")) //
                .add(Restrictions.eq("dataFeed", datafeed)) //
                .add(Restrictions.eq("status", Status.Started)) //
                .setFirstResult(0).setMaxResults(1) //
                .uniqueResult(); //
        if (res == null) {
            return null;
        }
        return (DataFeedExecution) res;
    }

    @Override
    public List<DataFeedExecution> findByDataFeed(DataFeed datafeed) {
        Session session = getSessionFactory().getCurrentSession();
        Class<DataFeedExecution> entityClz = getEntityClass();
        @SuppressWarnings("unchecked")
        List<DataFeedExecution> res = session.createCriteria(entityClz) //
                .add(Restrictions.eq("dataFeed", datafeed)) //
                .list(); //
        return res;
    }

}
