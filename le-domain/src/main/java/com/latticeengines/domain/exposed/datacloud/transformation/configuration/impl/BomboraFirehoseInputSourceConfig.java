package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;


import com.latticeengines.domain.exposed.datacloud.EngineConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.FileInputSourceConfig;

public class BomboraFirehoseInputSourceConfig extends FileInputSourceConfig {
    @Override
    public String getQualifier() {
        return "\"";
    }

    @Override
    public String getDelimiter() {
        return ",";
    }

    @Override
    public String getExtension() {
        return EngineConstants.CSV_GZ;
    }

    @Override
    public String getCharset() {
        return null;
    }
}