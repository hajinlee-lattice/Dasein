package com.latticeengines.datafabric.connector.hdfs;

import io.confluent.connect.hdfs.HdfsSinkConnector;

public class ErrorEntityToHdfsSinkConnector extends HdfsSinkConnector {

    @Override
    public String version() {
        return ErrorEntityToHdfsSinkConstants.VERSION;
    }

}
