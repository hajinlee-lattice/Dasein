package com.latticeengines.propdata.core.service;

import java.util.List;

import com.latticeengines.propdata.core.datasource.DataSourceConnection;
import com.latticeengines.propdata.core.datasource.DataSourcePool;
import com.latticeengines.propdata.core.source.Source;

public interface ZkConfigurationService {

    boolean refreshJobEnabled(Source source);

    String refreshCronSchedule(Source source);

    List<DataSourceConnection> getConnectionsInPool(DataSourcePool pool);

    Integer maxRealTimeInput();
}
