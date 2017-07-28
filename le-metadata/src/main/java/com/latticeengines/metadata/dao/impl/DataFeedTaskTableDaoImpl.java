package com.latticeengines.metadata.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.Table;
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
    public Table peekFirstDataTable(DataFeedTask task) {
        DataFeedTaskTable dataFeedTaskTable = peekFirstElement(task);
        if (dataFeedTaskTable == null) {
            return null;
        }
        return dataFeedTaskTable.getTable();
    }

    private DataFeedTaskTable peekFirstElement(DataFeedTask task) {
        Session session = getSessionFactory().getCurrentSession();
        Class<DataFeedTaskTable> entityClz = getEntityClass();
        Object res = session.createCriteria(entityClz) //
                .add(Restrictions.eq("dataFeedTask", task)) //
                .setFirstResult(0).setMaxResults(1) //
                .uniqueResult(); //
        if (res == null) {
            return null;
        }
        return (DataFeedTaskTable) res;
    }

    @Override
    public Table pollFirstDataTable(DataFeedTask task) {
        DataFeedTaskTable dataFeedTaskTable = peekFirstElement(task);
        if (dataFeedTaskTable == null) {
            return null;
        }
        Table table = dataFeedTaskTable.getTable();
        if (table == null) {
            return null;
        }
        delete(dataFeedTaskTable);
        return table;
    }

    @Override
    public List<DataFeedTaskTable> getDataFeedTaskTables(DataFeedTask task) {
        Session session = getSessionFactory().getCurrentSession();
        Class<DataFeedTaskTable> entityClz = getEntityClass();
        @SuppressWarnings("unchecked")
        List<DataFeedTaskTable> datafeedTaskTables = session.createCriteria(entityClz) //
                .add(Restrictions.eq("dataFeedTask", task)) //
                .list(); //
        return datafeedTaskTables;
    }

    @Override
    public void deleteDataFeedTaskTables(DataFeedTask datafeedTask) {
        Session session = getSessionFactory().getCurrentSession();
        Class<DataFeedTaskTable> entityClz = getEntityClass();
        String queryStr = String.format(
                "Delete from %s where FK_TASK_ID =:pid", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setLong("pid", datafeedTask.getPid());
        query.executeUpdate();
    }
}
