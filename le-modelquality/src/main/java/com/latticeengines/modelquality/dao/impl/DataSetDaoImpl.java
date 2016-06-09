package com.latticeengines.modelquality.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.modelquality.dao.DataSetDao;

@Component("dataSetDao")
public class DataSetDaoImpl extends BaseDaoImpl<DataSet> implements DataSetDao {

    @Override
    protected Class<DataSet> getEntityClass() {
        return DataSet.class;
    }

}
