package com.latticeengines.datafabric.functionalframework;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-datafabric-connect-context.xml" })
public abstract class DataFabricConnectFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    protected static final Logger log = LoggerFactory.getLogger(DataFabricConnectFunctionalTestNGBase.class);

    protected static final String BASE_DIR = "/Pods/Default/Services/PropData/Sources";

    @Autowired
    protected FabricMessageService messageService;

    @Autowired
    protected FabricDataService dataService;

    @Autowired
    protected Configuration conf;

}
