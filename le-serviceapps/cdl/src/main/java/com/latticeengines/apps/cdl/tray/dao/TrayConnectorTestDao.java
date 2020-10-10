package com.latticeengines.apps.cdl.tray.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTest;

public interface TrayConnectorTestDao extends BaseDao<TrayConnectorTest> {

    List<TrayConnectorTest> findUnfinishedTests();

}
