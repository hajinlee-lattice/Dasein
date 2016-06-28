package com.latticeengines.modelquality.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelquality.DataFlow;
import com.latticeengines.modelquality.dao.DataFlowDao;

@Component("dataFlowDao")
public class DataFlowDaoImpl extends BaseDaoImpl<DataFlow> implements DataFlowDao {

    @Override
    protected Class<DataFlow> getEntityClass() {
        return DataFlow.class;
    }

}
