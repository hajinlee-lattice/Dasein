package com.latticeengines.upgrade.functionalframework;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-upgrade-context.xml" })
public class UpgradeFunctionalTestNGBase  extends AbstractTestNGSpringContextTests {

    protected static final String CUSTOMER = "Nutanix_PLS132";
    protected static final String TUPLE_ID = "Nutanix_PLS132.Nutanix_PLS132.Production";
    protected static final String MODEL_GUID = "ms__5d074f72-c8f0-4d53-aebc-912fb066daa0-PLSModel";
    protected static final String UUID = "5d074f72-c8f0-4d53-aebc-912fb066daa0";
    protected static final String EVENT_TABLE = "Q_PLS_Modeling_Nutanix_PLS132";
    protected static final String CONTAINER_ID = "1416355548818_20011";

    @Value("${dataplatform.customer.basedir}")
    protected String customerBase;

}
