package com.latticeengines.datacloud.etl.service;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public interface HiveTableService {

    void createTable(String sourceName, String version);

    void createTable(String tableName, CustomerSpace customerSpace, String namespace);

}
