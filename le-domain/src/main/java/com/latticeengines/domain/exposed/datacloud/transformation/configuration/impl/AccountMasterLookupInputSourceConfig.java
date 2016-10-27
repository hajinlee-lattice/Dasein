package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.latticeengines.domain.exposed.datacloud.transformation.configuration.InputSourceConfig;

public class AccountMasterLookupInputSourceConfig implements InputSourceConfig {
    private String version;

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

}
