package com.latticeengines.datafabric.functionalframework;

import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.datafabric.service.datastore.FabricDataService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-datafabric-context.xml" })
public abstract class DataFabricFunctionalTestNGBase extends AbstractTestNGSpringContextTests {


    @SuppressWarnings("unused")
    protected static final Log log = LogFactory.getLog(DataFabricFunctionalTestNGBase.class);

    @Autowired
    protected FabricMessageService messageService;

    @Autowired
    protected FabricDataService dataService;

}

