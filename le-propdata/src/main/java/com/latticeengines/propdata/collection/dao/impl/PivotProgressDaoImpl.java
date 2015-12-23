package com.latticeengines.propdata.collection.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.collection.PivotProgress;
import com.latticeengines.propdata.collection.dao.PivotProgressDao;
import com.latticeengines.propdata.collection.source.impl.PivotedSource;

@Component("pivotProgressDao")
public class PivotProgressDaoImpl extends ProgressDaoImplBase<PivotProgress>
        implements PivotProgressDao {

    @Override
    protected Class<PivotProgress> getEntityClass() {
        return PivotProgress.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public PivotProgress findByBaseSourceVersion(PivotedSource source, String baseSourceVersion) {
        String sourceName = source.getSourceName();
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format(
                "from %s where SourceName = :sourceName and BaseSourceVersion = :version",
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("sourceName", sourceName);
        query.setString("version", baseSourceVersion);
        List<PivotProgress> list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return list.get(0);
    }

}
