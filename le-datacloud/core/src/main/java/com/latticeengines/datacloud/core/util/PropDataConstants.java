package com.latticeengines.datacloud.core.util;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public final class PropDataConstants {

    protected PropDataConstants() {
        throw new UnsupportedOperationException();
    }

    public static final String SERVICE_CUSTOMERSPACE = CustomerSpace.parse("PropDataService")
            .toString();
    public static final String SCAN_SUBMITTER = "RESTScanner";

}
