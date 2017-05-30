package com.latticeengines.metadata.dao.impl;

import java.util.List;

import javax.persistence.JoinColumn;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.DataFeedTaskTable;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.metadata.dao.DataFeedTaskTableDao;

@Component("datafeedTaskTableDao")
public class DataFeedTaskTableDaoImpl extends BaseDaoImpl<DataFeedTaskTable> implements DataFeedTaskTableDao {

    @Override
    protected Class<DataFeedTaskTable> getEntityClass() {
        return DataFeedTaskTable.class;
    }

    @Override
    public Table peekFirstDataTable(Long taskPid) {
        Session session = getSessionFactory().getCurrentSession();
        Class<DataFeedTaskTable> entityClz = getEntityClass();
        String queryStr = String.format("from %s where dataFeedTask = :taskId", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr).setFirstResult(0).setMaxResults(1);
        query.setLong("taskId", taskPid);
        List<?> list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return ((DataFeedTaskTable) list.get(0)).getTable();
    }

    @Override
    public Table pollFirstDataTable(Long taskPid) {
        Session session = getSessionFactory().getCurrentSession();
        Class<DataFeedTaskTable> entityClz = getEntityClass();
        Table table = peekFirstDataTable(taskPid);
        if (table == null) {
            return null;
        }
        String queryStr = String.format("delete from %s where dataFeedTask = :taskId AND table = :tableId",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setLong("taskId", taskPid);
        query.setLong("tableId", table.getPid());
        query.executeUpdate();
        return table;
    }

    @Override
    public void addDataTable(Long taskPid, Long tablePid) throws Exception {
        Session session = getSessionFactory().getCurrentSession();
        Class<DataFeedTaskTable> entityClz = getEntityClass();
        String queryStr = String.format("insert into %s (%s, %s) values (:taskId, :tableId)",
                entityClz.getAnnotation(javax.persistence.Table.class).name(),
                entityClz.getDeclaredField("dataFeedTask").getAnnotation(JoinColumn.class).name(),
                entityClz.getDeclaredField("table").getAnnotation(JoinColumn.class).name());
        Query query = session.createSQLQuery(queryStr);
        query.setLong("taskId", taskPid);
        query.setLong("tableId", tablePid);
        query.executeUpdate();
    }

    @Override
    public void clearTableQueue() {
        Session session = getSessionFactory().getCurrentSession();
        Class<DataFeedTaskTable> entityClz = getEntityClass();
        String queryStr = String.format("delete from %s",
                entityClz.getAnnotation(javax.persistence.Table.class).name());
        Query query = session.createSQLQuery(queryStr);
        query.executeUpdate();
    }
}
