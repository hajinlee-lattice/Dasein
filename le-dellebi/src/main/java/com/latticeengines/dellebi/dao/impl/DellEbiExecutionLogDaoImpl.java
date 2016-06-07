package com.latticeengines.dellebi.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.dellebi.dao.DellEbiExecutionLogDao;
import com.latticeengines.domain.exposed.dellebi.DellEbiExecutionLog;

@Component("dellEbiExecutionLogDao")
public class DellEbiExecutionLogDaoImpl extends BaseDaoImpl<DellEbiExecutionLog>implements DellEbiExecutionLogDao {

    @Override
    protected Class<DellEbiExecutionLog> getEntityClass() {
        return DellEbiExecutionLog.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public DellEbiExecutionLog getEntryByFile(String file) {
        Session session = getSessionFactory().getCurrentSession();

        Class<DellEbiExecutionLog> entityClz = getEntityClass();
        String queryStr = String.format("from %s where FileName = :file order by id desc", entityClz.getSimpleName());

        Query query = session.createQuery(queryStr);
        query.setString("file", file);
        List<DellEbiExecutionLog> list = query.list();
        if (list.size() == 0) {
            return null;
        }

        return list.get(0);
    }

}
