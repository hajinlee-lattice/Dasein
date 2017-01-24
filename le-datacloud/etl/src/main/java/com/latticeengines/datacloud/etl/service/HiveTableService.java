package com.latticeengines.datacloud.etl.service;

public interface HiveTableService {

    void createTable(String sourceName, String version);

}
