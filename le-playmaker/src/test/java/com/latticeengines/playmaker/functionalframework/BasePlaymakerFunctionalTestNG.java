package com.latticeengines.playmaker.functionalframework;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.playmaker.entitymgr.PlaymakerTenantEntityMgr;
import com.latticeengines.playmaker.entitymgr.impl.PlaymakerTenantEntityMgrImplTestNG;

@ContextConfiguration(locations = { "classpath:test-playmaker-context.xml" })
public class BasePlaymakerFunctionalTestNG extends AbstractTestNGSpringContextTests {

    @Value("${playmaker.api.hostport}")
    protected String apiHostPort;

    @Value("${playmaker.auth.hostport}")
    protected String authHostPort;

    protected PlaymakerTenant tenant;

    @Autowired
    protected PlaymakerTenantEntityMgr playMakerEntityMgr;

    public void beforeClass() {
        tenant = PlaymakerTenantEntityMgrImplTestNG.getTenant();

        try {
            playMakerEntityMgr.deleteByTenantName(tenant.getTenantName());
        } catch (Exception ex) {
            System.out.println("Warning=" + ex.getMessage());
        }
    }

}
