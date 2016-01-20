package com.latticeengines.propdata.collection.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.RefreshProgress;
import com.latticeengines.propdata.collection.dao.RefreshProgressDao;
import com.latticeengines.propdata.core.source.ServingSource;

@Component("refreshProgressDao")
public class RefreshProgressDaoImpl extends ProgressDaoImplBase<RefreshProgress>
        implements RefreshProgressDao {

    @Override
    protected Class<RefreshProgress> getEntityClass() {
        return RefreshProgress.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public RefreshProgress findByBaseSourceVersion(ServingSource source, String baseSourceVersion) {
        String sourceName = source.getSourceName();
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format(
                "from %s where SourceName = :sourceName and BaseSourceVersion = :version",
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("sourceName", sourceName);
        query.setString("version", baseSourceVersion);
        List<RefreshProgress> list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return list.get(0);
    }

}
