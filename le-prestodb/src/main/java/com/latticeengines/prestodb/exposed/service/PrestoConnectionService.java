package com.latticeengines.prestodb.exposed.service;

import javax.sql.DataSource;

public interface PrestoConnectionService {

    DataSource getPrestoDataSource();

    String getClusterId();

    boolean isPrestoDbAvailable();

}
