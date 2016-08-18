package com.latticeengines.propdata.engine.transformation.configuration.impl;

import com.latticeengines.propdata.engine.common.EngineConstants;
import com.latticeengines.propdata.engine.transformation.configuration.FileInputSourceConfig;

public class DnBCacheSeedInputSourceConfig extends FileInputSourceConfig {

    @Override
    public String getQualifier() {
        return null;
    }

    @Override
    public String getDelimiter() {
        return "|";
    }

    @Override
    public String getExtension() {
        return EngineConstants.OUT_GZ;
    }

    @Override
    public String getCharset() {
        return "ISO-8859-1";
    }
}