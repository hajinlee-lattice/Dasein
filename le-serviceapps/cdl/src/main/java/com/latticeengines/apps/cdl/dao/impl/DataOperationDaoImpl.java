package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.DataOperationDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.DataOperation;

@Component("dataOperationDao")
public class DataOperationDaoImpl extends BaseDaoImpl<DataOperation> implements DataOperationDao {

    @Override
    protected Class<DataOperation> getEntityClass() {
        return DataOperation.class;
    }
}
