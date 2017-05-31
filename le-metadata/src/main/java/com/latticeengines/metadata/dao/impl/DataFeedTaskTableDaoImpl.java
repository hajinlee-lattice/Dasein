package com.latticeengines.metadata.dao.impl;

import java.util.List;

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
        DataFeedTaskTable dataFeedTaskTable = peekFirstElement(taskPid);
        if (dataFeedTaskTable == null) {
            return null;
        }
        return dataFeedTaskTable.getTable();
    }

    private DataFeedTaskTable peekFirstElement(Long taskPid) {
        Session session = getSessionFactory().getCurrentSession();
        Class<DataFeedTaskTable> entityClz = getEntityClass();
        String queryStr = String.format("from %s where dataFeedTask = :taskId", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr).setFirstResult(0).setMaxResults(1);
        query.setLong("taskId", taskPid);
        List<?> list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return ((DataFeedTaskTable) list.get(0));
    }

    @Override
    public Table pollFirstDataTable(Long taskPid) {
        DataFeedTaskTable dataFeedTaskTable = peekFirstElement(taskPid);
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
}
