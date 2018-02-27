package com.latticeengines.metadata.dao.impl;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTaskTable;
import com.latticeengines.metadata.dao.DataFeedTaskTableDao;

@Component("datafeedTaskTableDao")
public class DataFeedTaskTableDaoImpl extends BaseDaoImpl<DataFeedTaskTable> implements DataFeedTaskTableDao {

    @Override
    protected Class<DataFeedTaskTable> getEntityClass() {
        return DataFeedTaskTable.class;
    }

    @Override
    public void deleteDataFeedTaskTables(DataFeedTask datafeedTask) {
        Session session = getSessionFactory().getCurrentSession();
        Class<DataFeedTaskTable> entityClz = getEntityClass();
        String queryStr = String.format("Delete from %s where FK_TASK_ID =:pid", entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("pid", datafeedTask.getPid());
        query.executeUpdate();
    }
}
