package com.latticeengines.domain.exposed.serviceapps.lp;

import com.latticeengines.domain.exposed.serviceapps.core.BootstrapRequest;

public class LPBootstrapRequest extends BootstrapRequest {

    @Override
    public String getApplication() {
        return LP;
    }

}
