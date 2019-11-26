package com.latticeengines.metadata.dao.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.google.common.collect.Iterables;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.RetentionPolicyUtil;
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
        query.setParameter("tableName", name);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (Table) list.get(0);
    }

    @Override
    public List<Table> findAllWithExpiredRetentionPolicy(int index, int max) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Table> entityClz = getEntityClass();
        String queryStr = String.format("from %s where RETENTION_POLICY != :noExpirePolicy order by pid", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("noExpirePolicy", RetentionPolicyUtil.NEVER_EXPIRE_POLICY);
        query.setFirstResult(index);
        query.setMaxResults(max);
        List list = query.list();
        return list;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public List<Table> findByNames(Set<String> names) {
        Session session = getSessionFactory().getCurrentSession();
        Iterable<List<String>> namesList = Iterables.partition(names, DEFAULT_JDBC_FETCH_SIZE);
        List<Table> result = new ArrayList<>();
        namesList.forEach(
                namesToQuery -> {
                    Class<Table> entityClz = getEntityClass();
                    String queryStr = String.format("from %s where name in :tableNames", entityClz.getSimpleName());
                    Query query = session.createQuery(queryStr);
                    query.setParameter("tableNames", namesToQuery);
                    List list = query.list();
                    result.addAll(list);
                }
        );
        return result;
    }
}
