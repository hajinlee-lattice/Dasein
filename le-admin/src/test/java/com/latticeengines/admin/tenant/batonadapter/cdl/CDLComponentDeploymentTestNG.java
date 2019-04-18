package com.latticeengines.admin.tenant.batonadapter.cdl;

import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.latticeengines.admin.tenant.batonadapter.BatonAdapterDeploymentTestNGBase;
import com.latticeengines.admin.tenant.batonadapter.pls.PLSComponent;
import com.latticeengines.admin.tenant.batonadapter.pls.PLSComponentDeploymentTestNG;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;

@Component
public class CDLComponentDeploymentTestNG extends BatonAdapterDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CDLComponentDeploymentTestNG.class);

    @Value("${common.test.microservice.url}")
    private String microserviceUrl;

    @Inject
    private PLSComponentDeploymentTestNG plsComponentDeploymentTestNG;

    @SuppressWarnings("unused")
    private String cdlUrl;

    @PostConstruct
    public void postConstruct() {
        cdlUrl = microserviceUrl + "/cdl";
    }

    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
        log.info(
                "Start tearing down public class CDLComponentDeploymentTestNG extends BatonAdapterDeploymentTestNGBase");
        super.tearDown();
        plsComponentDeploymentTestNG.tearDown();
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "deployment")
    public void testInstallation() {
        loginAD();
        bootstrap(contractId, tenantId, PLSComponent.componentName,
                plsComponentDeploymentTestNG.getPLSDocumentDirectory());
        waitUntilStateIsNotInitial(contractId, tenantId, PLSComponent.componentName);
        bootstrap(contractId, tenantId, CDLComponent.componentName,
                batonService.getDefaultConfiguration(getServiceName()));
        BootstrapState state = waitUntilStateIsNotInitial(contractId, tenantId, CDLComponent.componentName);
        Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);
        state = waitUntilStateIsNotInitial(contractId, tenantId, PLSComponent.componentName);
        Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);

        String customerSpace = String.format("%s.%s.%s", contractId, tenantId,
                CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);
        loginAndAttachPls(testAdminUsername, testAdminPassword, customerSpace);
        Map map = plsRestTemplate.postForObject(getPlsHostPort() + "/pls/entities/counts", new FrontEndQuery(),
                Map.class);
        Assert.assertTrue(MapUtils.isNotEmpty(map));
        Map<BusinessEntity, Long> counts = JsonUtils.convertMap(map, BusinessEntity.class, Long.class);
        counts.forEach((entity, count) -> Assert.assertEquals(count, new Long(0)));
    }

    @Override
    protected String getServiceName() {
        return CDLComponent.componentName;
    }

}
