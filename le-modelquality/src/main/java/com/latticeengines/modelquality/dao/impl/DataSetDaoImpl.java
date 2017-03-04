package com.latticeengines.modelquality.dao.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.modelquality.dao.DataSetDao;
import com.latticeengines.modelquality.service.impl.FileModelRunServiceImpl;

@Component("dataSetDao")
public class DataSetDaoImpl extends BaseDaoImpl<DataSet> implements DataSetDao {

    @Override
    protected Class<DataSet> getEntityClass() {
        return DataSet.class;
    }

    private static final Log log = LogFactory.getLog(FileModelRunServiceImpl.class);

    @Override
    public DataSet findByTenantAndTrainingSet(String tenantID, String trainingSetFilePath) {

        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format("from %s where %s = :tenantID and %s = :trainingHdfsPath",
                getEntityClass().getSimpleName(), "CUSTOMER_SPACE", "TRAINING_HDFS_PATH");
        Query query = session.createQuery(queryStr);
        query.setParameter("tenantID", tenantID);
        query.setParameter("trainingHdfsPath", trainingSetFilePath);
        @SuppressWarnings("unchecked")
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
