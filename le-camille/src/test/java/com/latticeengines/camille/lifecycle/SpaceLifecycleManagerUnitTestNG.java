package com.latticeengines.camille.lifecycle;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.latticeengines.camille.exposed.CamilleConfiguration;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.lifecycle.SpaceLifecycleManager;
import com.latticeengines.camille.exposed.lifecycle.TenantLifecycleManager;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;

import junit.framework.Assert;

public class SpaceLifecycleManagerUnitTestNG {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private static final String contractId = CamilleTestEnvironment.getContractId();
    private static final String tenantId = CamilleTestEnvironment.getTenantId();
    private static final CustomerSpaceInfo customerSpaceInfo = CamilleTestEnvironment.getCustomerSpaceInfo();

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        CamilleEnvironment.start(CamilleEnvironment.Mode.RUNTIME, new CamilleConfiguration("Production", "10.51.1.217:2181,10.51.1.218:2181,10.51.1.219:2181"));
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        CamilleEnvironment.stop();
    }

    @Test(groups = "unit")
    public void testGetAll() throws Exception {
/*
        List<String> lst = new ArrayList<String>();
        lst.add("VeracodePOC");
        fixit(lst);
*/
//        SpaceLifecycleManager.getInfo("VeracodePOC", "VeracodePOC", "Production");
        SpaceLifecycleManager.getAll();
    }

    private void fixit(List<String> toFix) {
        for (String space : toFix) {
            try {
                SpaceLifecycleManager.create(space, space, "Production", new CustomerSpaceInfo(new CustomerSpaceProperties(space, space, null, null), ""));
            }
            catch (Exception e) {
                log.error("Failed", e);
            }

        }
    }


}
