package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.latticeengines.domain.exposed.datacloud.EngineConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.FileInputSourceConfig;

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