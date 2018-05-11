package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.DataCollectionStatusDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;

@Component("dataCollectionStatusDao")
public class DataCollectionStatusDaoImpl extends BaseDaoImpl<DataCollectionStatus> implements DataCollectionStatusDao {

    protected Class<DataCollectionStatus> getEntityClass() {
        return DataCollectionStatus.class;
    }
}
