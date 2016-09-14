package com.latticeengines.modelquality.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.modelquality.DataFlow;
import com.latticeengines.modelquality.dao.DataFlowDao;
import com.latticeengines.modelquality.entitymgr.DataFlowEntityMgr;

@Component("dataFlowEntityMgr")
public class DataFlowEntityMgrImpl extends BaseEntityMgrImpl<DataFlow> implements DataFlowEntityMgr {
    
    @Autowired
    private DataFlowDao dataFlowDao;

    @Override
    public BaseDao<DataFlow> getDao() {
        return dataFlowDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void createDataFlows(List<DataFlow> dataflows) {
        for (DataFlow dataflow : dataflows) {
            dataFlowDao.create(dataflow);
        }
    }

    @Override
    public DataFlow findByName(String dataFlowName) {
        return dataFlowDao.findByField("NAME", dataFlowName);
    }

}
