package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.DataIntegrationStatusMessageDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMessage;

@Component("dataIntegrationStatusMessageDao")
public class DataIntegrationStatusMessageDaoImpl extends BaseDaoImpl<DataIntegrationStatusMessage>
        implements DataIntegrationStatusMessageDao {

    @Override
    protected Class<DataIntegrationStatusMessage> getEntityClass() {
        return DataIntegrationStatusMessage.class;
    }

}
