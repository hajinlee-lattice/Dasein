package com.latticeengines.datacloud.core.dao.impl;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.dao.DataCloudVersionDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;

@Component("dataCloudVersionDao")
public class DataCloudVersionDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<DataCloudVersion>
        implements DataCloudVersionDao {

    @Override
    protected Class<DataCloudVersion> getEntityClass() {
        return DataCloudVersion.class;
    }

    public DataCloudVersion latestApprovedForMajorVersion(String majorVersion) {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format(
                "from %s where MajorVersion = :majorVersion and Status = '%s' order by CONVERT(SUBSTRING_INDEX(Version, '.', -1), UNSIGNED) desc",
                getEntityClass().getSimpleName(), DataCloudVersion.Status.APPROVED);
        Query<DataCloudVersion> query = session.createQuery(queryStr, DataCloudVersion.class);
        query.setParameter("majorVersion", majorVersion);
        List<?> results = query.list();
        if (results == null || results.isEmpty()) {
            return null;
        } else {
            return (DataCloudVersion) query.list().get(0);
        }
    }

    public List<String> allApprovedMajorVersions() {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format(
                "select distinct majorVersion from %s where Status = '%s' order by MajorVersion desc",
                getEntityClass().getSimpleName(), DataCloudVersion.Status.APPROVED);
        Query<String> query = session.createQuery(queryStr, String.class);
        List<String> results = (List<String>) query.list();
        if (CollectionUtils.isEmpty(results)) {
            return null;
        } else {
            return results;
        }
    }

    public List<DataCloudVersion> allApprovedVerions() {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s where Status = '%s'", getEntityClass().getSimpleName(),
                DataCloudVersion.Status.APPROVED);
        Query<DataCloudVersion> query = session.createQuery(queryStr, DataCloudVersion.class);
        return (List<DataCloudVersion>) query.list();
    }

}
