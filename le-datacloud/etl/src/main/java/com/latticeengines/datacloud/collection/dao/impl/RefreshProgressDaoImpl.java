package com.latticeengines.datacloud.collection.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.dao.RefreshProgressDao;
import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.domain.exposed.datacloud.manage.RefreshProgress;

@Component("refreshProgressDao")
public class RefreshProgressDaoImpl extends ProgressDaoImplBase<RefreshProgress>
        implements RefreshProgressDao {

    @Override
    protected Class<RefreshProgress> getEntityClass() {
        return RefreshProgress.class;
    }

    @Override
    public RefreshProgress findByBaseSourceVersion(DerivedSource source, String baseSourceVersion) {
        String sourceName = source.getSourceName();
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format(
                "from %s where SourceName = :sourceName and BaseSourceVersion = :version",
                getEntityClass().getSimpleName());
        Query<RefreshProgress> query = session.createQuery(queryStr, RefreshProgress.class);
        query.setParameter("sourceName", sourceName);
        query.setParameter("version", baseSourceVersion);
        List<RefreshProgress> list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return list.get(0);
    }

}
