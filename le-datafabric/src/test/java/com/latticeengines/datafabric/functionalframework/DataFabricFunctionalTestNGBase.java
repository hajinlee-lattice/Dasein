package com.latticeengines.datafabric.functionalframework;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-datafabric-context.xml" })
public abstract class DataFabricFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    protected static final Log log = LogFactory.getLog(DataFabricFunctionalTestNGBase.class);

    @Autowired
    protected FabricMessageService messageService;

    @Autowired
    protected FabricDataService dataService;

}

