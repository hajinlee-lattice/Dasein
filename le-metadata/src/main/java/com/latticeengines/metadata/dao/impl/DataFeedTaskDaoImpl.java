package com.latticeengines.metadata.dao.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
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

    @SuppressWarnings("rawtypes")
    @Override
    public DataFeedTask getDataFeedTask(String source, String datafeedType, String entity, Long dataFeed) {
        Session session = sessionFactory.getCurrentSession();
        Class<DataFeedTask> entityClz = getEntityClass();
        String queryStr = String.format("from %s where feedType = :feedType and entity = :entity and "
                + "source = :source and dataFeed = :dataFeed", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("feedType", datafeedType);
        query.setString("entity", entity);
        query.setString("source", source);
        query.setLong("dataFeed", dataFeed);
        List list = query.list();
        // List<DataFeedTask> result = new ArrayList<>();
        if (list.size() == 0) {
            return null;
        }
        return (DataFeedTask) list.get(0);
    }

    @Override
    public void update(DataFeedTask datafeedTask, Date startTime) {
        Session session = getSessionFactory().getCurrentSession();
        Class<DataFeedTask> entityClz = getEntityClass();
        String queryStr = String.format(
                "update %s datafeedtask set datafeedtask.startTime=:startTime where datafeedtask.pid=:pid",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setLong("pid", datafeedTask.getPid());
        query.setDate("startTime", startTime);
        query.executeUpdate();
    }

    @Override
    public void update(DataFeedTask datafeedTask, Status status, Date lastImported) {
        Session session = getSessionFactory().getCurrentSession();
        Class<DataFeedTask> entityClz = getEntityClass();
        String queryStr = String.format(
                "update %s datafeedtask set lastImported=:lastImported, status=:status where datafeedtask.pid=:pid",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setLong("pid", datafeedTask.getPid());
        query.setDate("lastImported", lastImported);
        query.setString("status", status.name());
        query.executeUpdate();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public List<DataFeedTask> getDataFeedTaskWithSameEntity(String entity, Long dataFeed) {
        Session session = sessionFactory.getCurrentSession();
        Class<DataFeedTask> entityClz = getEntityClass();
        String queryStr = String.format("from %s where entity = :entity and dataFeed = :dataFeed",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("entity", entity);
        query.setLong("dataFeed", dataFeed);
        List list = query.list();
        List<DataFeedTask> result = new ArrayList<>();
        if (list.size() == 0) {
            return null;
        } else {
            for (Object dataFeedTask : list) {
                result.add((DataFeedTask) dataFeedTask);
            }
            return result;
        }
    }
}
