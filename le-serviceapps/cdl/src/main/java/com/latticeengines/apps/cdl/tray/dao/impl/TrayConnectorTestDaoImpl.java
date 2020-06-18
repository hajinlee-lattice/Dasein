package com.latticeengines.apps.cdl.tray.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.tray.dao.TrayConnectorTestDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTest;

@Component("trayConnectorTestDao")
public class TrayConnectorTestDaoImpl extends BaseDaoImpl<TrayConnectorTest> implements TrayConnectorTestDao {

    @Override
    protected Class<TrayConnectorTest> getEntityClass() {
        return TrayConnectorTest.class;
    }

}
