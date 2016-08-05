package com.latticeengines.propdata.engine.transformation.configuration.impl;

import com.latticeengines.propdata.core.PropDataConstants;
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
        return PropDataConstants.CSV_GZ;
    }

    @Override
    public String getCharset() {
        return null;
    }
}
