package com.latticeengines.upgrade.functionalframework;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-upgrade-context.xml" })
public class UpgradeFunctionalTestNGBase  extends AbstractTestNGSpringContextTests {

    protected static final String CUSTOMER_BASE = "/user/s-analytics/customers";
    protected static final String CUSTOMER = "Lattice_Relaunch";
    protected static final String MODEL_GUID = "ms__b99ddcc6-7ecb-45a0-b128-9664b51c1ce9-PLSModel";
    protected static final String UUID = "b99ddcc6-7ecb-45a0-b128-9664b51c1ce9";
    protected static final String EVENT_TABLE = "Q_PLS_Modeling_Lattice_Relaunch";
    protected static final String CONTAINER_ID = "1425511391553_3443";

    @Value("${dataplatform.customer.basedir}")
    protected String customerBase;

}
