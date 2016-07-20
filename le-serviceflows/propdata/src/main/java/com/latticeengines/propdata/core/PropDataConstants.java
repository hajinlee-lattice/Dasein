package com.latticeengines.propdata.core;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public final class PropDataConstants {

    public static final String PDSERVICE_TENANT = "PropDataService";
    public static final String SERVICE_CUSTOMERSPACE = CustomerSpace.parse(PDSERVICE_TENANT)
            .toString();
    public static final String SCAN_SUBMITTER = "RESTScanner";
    public static final String CSV_GZ = ".csv.gz";
    public static final String OUT_GZ = ".out.gz";
    public static final String GZ = ".gz";
}
