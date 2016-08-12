package com.latticeengines.propdata.engine.transformation.configuration.impl;

import com.latticeengines.propdata.engine.common.EngineConstants;
import com.latticeengines.propdata.engine.transformation.configuration.FileInputSourceConfig;

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