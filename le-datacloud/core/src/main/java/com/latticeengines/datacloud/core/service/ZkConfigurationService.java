package com.latticeengines.datacloud.core.service;

import java.util.List;

import com.latticeengines.datacloud.core.datasource.DataSourceConnection;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataSourcePool;

public interface ZkConfigurationService {

    List<DataSourceConnection> getConnectionsInPool(DataSourcePool pool);

    boolean isMatchDebugEnabled(CustomerSpace customerSpace);

    boolean useRemoteDnBGlobal();

}
