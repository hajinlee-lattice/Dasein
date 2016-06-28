package com.latticeengines.modelquality.entitymgr.impl;

import java.util.List;

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
    public void createDataSets(List<DataSet> datasets) {
        for (DataSet dataSet : datasets) {
            setupDataSet(dataSet);
            dataSetDao.create(dataSet);
        }
    }

    private void setupDataSet(DataSet dataSet) {
        List<ScoringDataSet> scoringDataSets = dataSet.getScoringDataSets();
        if (scoringDataSets != null) {
            for (ScoringDataSet scoringDataSet : scoringDataSets) {
                scoringDataSet.setDataSet(dataSet);
            }
        }
    }

}
