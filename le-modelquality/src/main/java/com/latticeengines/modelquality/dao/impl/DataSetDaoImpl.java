package com.latticeengines.modelquality.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.modelquality.dao.DataSetDao;

@Component("dataSetDao")
public class DataSetDaoImpl extends ModelQualityBaseDaoImpl<DataSet> implements DataSetDao {

    private static final Logger log = LoggerFactory.getLogger(DataSetDaoImpl.class);

    @Override
    protected Class<DataSet> getEntityClass() {
        return DataSet.class;
    }

    @Override
    public DataSet findByTenantAndTrainingSet(String tenantID, String trainingSetFilePath) {

        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format("from %s where %s = :tenantID and %s = :trainingHdfsPath",
                getEntityClass().getSimpleName(), "CUSTOMER_SPACE", "TRAINING_HDFS_PATH");
        Query<DataSet> query = session.createQuery(queryStr, DataSet.class);
        query.setParameter("tenantID", tenantID);
        query.setParameter("trainingHdfsPath", trainingSetFilePath);
        List<DataSet> results = query.list();
        if (results.size() == 0) {
            return null;
        }
        if (results.size() > 1) {
            // No need to throw an exception, just return the first one. But log
            // it
            log.info(String.format("Multiple rows found with given tenantID(%s) and trainingHdfsPath(%s)", tenantID,
                    trainingSetFilePath));
        }
        return results.get(0);
    }
}
