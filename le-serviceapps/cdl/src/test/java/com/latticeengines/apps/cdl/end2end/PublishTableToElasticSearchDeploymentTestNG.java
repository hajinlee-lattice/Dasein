package com.latticeengines.apps.cdl.end2end;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.elasticsearch.PublishTableToESRequest;
import com.latticeengines.proxy.exposed.cdl.PublishTableProxy;

public class PublishTableToElasticSearchDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(PublishTableToElasticSearchDeploymentTestNG.class);

    private String customerSpace;

    @Inject
    private PublishTableProxy publishTableProxy;

    @BeforeClass(groups = {"end2end"})
    @Override
    public void setup() throws Exception {
        super.setup();
        customerSpace = CustomerSpace.parse(mainCustomerSpace).getTenantId();
        resumeCheckpoint(ProcessAccountWithAdvancedMatchDeploymentTestNG.CHECK_POINT);
    }

    @Test(groups = "deployment")
    public void test() {
        PublishTableToESRequest request = new PublishTableToESRequest();
        
    }


}
