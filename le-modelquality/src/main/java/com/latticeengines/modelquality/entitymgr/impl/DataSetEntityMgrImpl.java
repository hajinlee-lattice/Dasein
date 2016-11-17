package com.latticeengines.modelquality.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.domain.exposed.modelquality.ScoringDataSet;
import com.latticeengines.modelquality.dao.DataSetDao;
import com.latticeengines.modelquality.entitymgr.DataSetEntityMgr;

@Component("dataSetEntityMgr")
public class DataSetEntityMgrImpl extends BaseEntityMgrImpl<DataSet> implements DataSetEntityMgr {

    @Autowired
    private DataSetDao dataSetDao;

    @Override
    public BaseDao<DataSet> getDao() {
        return dataSetDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(DataSet dataSet) {
        dataSet.setName(dataSet.getName().replace('/', '_'));
        if (dataSet.getScoringDataSets() != null && dataSet.getScoringDataSets().size() > 0) {
            for (ScoringDataSet scoringDataSet : dataSet.getScoringDataSets()) {
                scoringDataSet.setDataSet(dataSet);
            }
        }
        dataSetDao.create(dataSet);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataSet findByName(String name) {
        return dataSetDao.findByField("NAME", name);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataSet findByTenantAndTrainingSet(String tenantID, String trainingSetFilePath) {
        return dataSetDao.findByTenantAndTrainingSet(tenantID, trainingSetFilePath);
    }

}
