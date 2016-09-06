package com.latticeengines.pls.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.pls.dao.SourceFileDao;

@Component("sourceFileDao")
public class SourceFileDaoImpl extends BaseDaoImpl<SourceFile> implements SourceFileDao {

    @Override
    protected Class<SourceFile> getEntityClass() {
        return SourceFile.class;
    }

    @Override
    public SourceFile findByName(String name) {
        return findByField("name", name);
    }

    @Override
    @SuppressWarnings("unchecked")
    public SourceFile findByApplicationId(String applicationId) {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format("from %s where %s = :value",
                getEntityClass().getSimpleName(), "applicationId");
        Query query = session.createQuery(queryStr);
        query.setParameter("value", applicationId);
        List<SourceFile> results = query.list();
        if (results.size() == 0) {
            return null;
        }
        return results.get(0);
    }

    @Override
    public List<SourceFile> findAllSourceFiles() {
        return findAll();
    }

    @Override
    public SourceFile findByTableName(String tableName) {
        return findByField("tableName", tableName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public SourceFile getByTableName(String tableName) {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format("from %s where %s = :value",
                getEntityClass().getSimpleName(), "tableName");
        Query query = session.createQuery(queryStr);
        query.setParameter("value", tableName);
        List<SourceFile> results = query.list();
        if (results.size() == 0) {
            return null;
        }
        return results.get(0);
    }
}
