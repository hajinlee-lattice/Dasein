package com.latticeengines.propdata.engine.transformation.configuration.impl;

import com.latticeengines.propdata.engine.transformation.configuration.InputSourceConfig;

public class BomboraFirehoseInputSourceConfig implements InputSourceConfig {
    private String version;

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
