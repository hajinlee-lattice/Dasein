package com.latticeengines.eai.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.eai.config.HttpClientConfig;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.service.EaiZKService;
import static org.testng.Assert.assertEquals;

public class EaiZKServiceImplTestNG extends EaiFunctionalTestNGBase {

    @Autowired
    private EaiZKService eaiZKService;

    private String customer = this.getClass().getSimpleName();

    private CustomerSpace customerSpace = CustomerSpace.parse(customer);

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        initZK(customer);
    }

    @AfterClass(groups = "functional")
    private void cleanUp() throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        camille.delete(PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), customer));
    }

    @Test(groups = "functional")
    public void testRetrieveHttpConfig() {
        HttpClientConfig config = eaiZKService.getHttpClientConfig(customerSpace.toString());
        assertEquals(config.getConnectTimeout(), 60000);
        assertEquals(config.getImportTimeout(), 3600000);
    }

    @Test(groups = "functional")
    public void testMissingHttpConfig() {
        try {
            eaiZKService.getHttpClientConfig("SomeNonExistCustomer");
        } catch (LedpException e) {
            assertEquals(e.getCode(), LedpCode.LEDP_17005);
            System.out.println(e);
        }
    }

}
