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
    
    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void create(DataSet dataSet) {
        for (ScoringDataSet scoringDataSet : dataSet.getScoringDataSets()) {
            scoringDataSet.setDataSet(dataSet);
        }
        super.create(dataSet);
    }

    @Override
    public DataSet findByName(String name) {
        return dataSetDao.findByField("NAME", name);
    }


}
