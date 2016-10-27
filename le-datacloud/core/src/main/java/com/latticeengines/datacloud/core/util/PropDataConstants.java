package com.latticeengines.datacloud.core.util;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public final class PropDataConstants {

    public static final String PDSERVICE_TENANT = "PropDataService";
    public static final String SERVICE_CUSTOMERSPACE = CustomerSpace.parse(PDSERVICE_TENANT)
            .toString();
    public static final String SCAN_SUBMITTER = "RESTScanner";
}