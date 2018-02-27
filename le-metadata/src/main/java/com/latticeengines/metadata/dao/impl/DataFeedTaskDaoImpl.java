package com.latticeengines.metadata.dao.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

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

    @SuppressWarnings("rawtypes")
    @Override
    public DataFeedTask getDataFeedTask(String source, String datafeedType, String entity, Long dataFeed) {
        Session session = sessionFactory.getCurrentSession();
        Class<DataFeedTask> entityClz = getEntityClass();
        String queryStr = String.format("from %s where feedType = :feedType and entity = :entity and "
                + "source = :source and dataFeed = :dataFeed", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("feedType", datafeedType);
        query.setParameter("entity", entity);
        query.setParameter("source", source);
        query.setParameter("dataFeed", dataFeed);
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
        query.setParameter("status", status.name());
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
        query.setParameter("entity", entity);
        query.setParameter("dataFeed", dataFeed);
        List<?> list = query.list();
        List<DataFeedTask> result = new ArrayList<>();
        if (list.size() == 0) {
            return null;
        } else {
            result = list.stream().map(entityClz::cast).collect(Collectors.toList());
            return result;
        }
    }
}
