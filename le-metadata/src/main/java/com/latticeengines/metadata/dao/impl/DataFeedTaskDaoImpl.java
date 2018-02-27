package com.latticeengines.metadata.dao.impl;

import java.util.Date;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask.Status;
import com.latticeengines.metadata.dao.DataFeedTaskDao;

@Component("datafeedTaskDao")
public class DataFeedTaskDaoImpl extends BaseDaoImpl<DataFeedTask> implements DataFeedTaskDao {

    @Override
    protected Class<DataFeedTask> getEntityClass() {
        return DataFeedTask.class;
    }

    @Override
    public void createDataFeedTask(DataFeedTask datafeedTask) {
        super.create(datafeedTask);
    }

    @Override
    public void update(DataFeedTask datafeedTask, Date startTime) {
        Session session = getSessionFactory().getCurrentSession();
        Class<DataFeedTask> entityClz = getEntityClass();
        String queryStr = String.format(
                "update %s datafeedtask set datafeedtask.startTime=:startTime where datafeedtask.pid=:pid",
                entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("pid", datafeedTask.getPid());
        query.setParameter("startTime", startTime);
        query.executeUpdate();
    }

    @Override
    public void update(DataFeedTask datafeedTask, Status status, Date lastImported) {
        Session session = getSessionFactory().getCurrentSession();
        Class<DataFeedTask> entityClz = getEntityClass();
        String queryStr = String.format(
                "update %s datafeedtask set lastImported=:lastImported, status=:status where datafeedtask.pid=:pid",
                entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("pid", datafeedTask.getPid());
        query.setParameter("lastImported", lastImported);
        query.setParameter("status", status);
        query.executeUpdate();
    }

}
