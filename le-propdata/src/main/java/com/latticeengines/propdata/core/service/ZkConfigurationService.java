package com.latticeengines.propdata.core.service;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.DataSourcePool;
import com.latticeengines.propdata.core.datasource.DataSourceConnection;

public interface ZkConfigurationService {

    Integer maxRealTimeInput();

    List<DataSourceConnection> getConnectionsInPool(DataSourcePool pool);

}
