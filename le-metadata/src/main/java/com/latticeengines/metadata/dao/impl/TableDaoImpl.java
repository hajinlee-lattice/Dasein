package com.latticeengines.metadata.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.metadata.dao.TableDao;

@Component("tableDao")
public class TableDaoImpl extends BaseDaoImpl<Table> implements TableDao {

    @Override
    protected Class<Table> getEntityClass() {
        return Table.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Table findByName(String name) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Table> entityClz = getEntityClass();
        String queryStr = String.format("from %s where name = :tableName", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("tableName", name);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (Table) list.get(0);
    }

}
