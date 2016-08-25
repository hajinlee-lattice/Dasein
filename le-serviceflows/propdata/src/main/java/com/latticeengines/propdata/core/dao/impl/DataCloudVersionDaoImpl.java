package com.latticeengines.propdata.core.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.propdata.manage.DataCloudVersion;
import com.latticeengines.propdata.core.dao.DataCloudVersionDao;

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
                "from %s where MajorVersion = :majorVersion and Status = '%s' order by CreateDate desc",
                getEntityClass().getSimpleName(), DataCloudVersion.Status.APPROVED);
        Query query = session.createQuery(queryStr);
        query.setString("majorVersion", majorVersion);
        List<?> results = query.list();
        if (results == null || results.isEmpty()) {
            return null;
        } else {
            return (DataCloudVersion) query.list().get(0);
        }
    }

}
