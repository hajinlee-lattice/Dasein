package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.DataCollectionStatusHistoryDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatusHistory;

@Component("dataCollectionStatusHistoryDao")
public class DataCollectionStatusHistoryDaoImpl extends BaseDaoImpl<DataCollectionStatusHistory>
        implements DataCollectionStatusHistoryDao {

    @Override
    protected Class<DataCollectionStatusHistory> getEntityClass() {
        return DataCollectionStatusHistory.class;
    }

}
