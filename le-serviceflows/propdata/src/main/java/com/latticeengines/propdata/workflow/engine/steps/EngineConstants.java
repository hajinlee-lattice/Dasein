package com.latticeengines.propdata.workflow.engine.steps;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public class EngineConstants {

    public static final CustomerSpace PRODATA_CUSTOMERSPACE = CustomerSpace.parse("PropDataService");
    public static final String SQOOP_CUSTOMER_PATTERN = "PropData~[%s]";

}
