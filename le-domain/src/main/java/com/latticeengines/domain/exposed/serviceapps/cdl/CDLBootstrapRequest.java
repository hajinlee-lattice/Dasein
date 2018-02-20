package com.latticeengines.domain.exposed.serviceapps.cdl;

import com.latticeengines.domain.exposed.serviceapps.core.BootstrapRequest;

public class CDLBootstrapRequest extends BootstrapRequest {

    @Override
    public String getApplication() {
        return CDL;
    }

}
