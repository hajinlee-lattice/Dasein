package com.latticeengines.metadata.service.impl;

import java.util.Collections;

import javax.inject.Inject;

import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.datastore.DataTemplate;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.DataTemplateService;

public class DataTemplateServiceImplTestNG extends MetadataFunctionalTestNGBase {

    @Inject
    private DataTemplateService dataTemplateService;

    private String testTenantId;

    private static final String DATATEMPLATE_NAME = "DataTempLate_Test";

    @BeforeClass(groups = "functional")
    public void setup() {
        functionalTestBed.bootstrap(1);
        Tenant testTenant = functionalTestBed.getMainTestTenant();
        MultiTenantContext.setTenant(testTenant);
        String customerSpace = CustomerSpace.parse(testTenant.getId()).toString();
        testTenantId = CustomerSpace.shortenCustomerSpace(customerSpace);

    }

    @Test(groups = "functional")
    public void testCrud() throws Exception {
        DataTemplate dataTemplate = new DataTemplate();
        dataTemplate.setName(DATATEMPLATE_NAME);
        dataTemplate.setTenant(testTenantId);
        String uuid = dataTemplateService.create(dataTemplate);
        Assert.assertNotNull(uuid);

        Thread.sleep(500);
        DataTemplate foundDataTemplate = dataTemplateService.findByUuid(uuid);
        Assert.assertNotNull(foundDataTemplate);
        Assert.assertEquals(foundDataTemplate.getName(), DATATEMPLATE_NAME);

        dataTemplate.setName("new_" + DATATEMPLATE_NAME);
        dataTemplateService.updateByUuid(uuid, dataTemplate);
        RetryTemplate retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AssertionError.class), null);
        retry.execute(context -> {
            DataTemplate tempDataTemplate = dataTemplateService.findByUuid(uuid);
            Assert.assertEquals(tempDataTemplate.getName(), "new_" + DATATEMPLATE_NAME);
            return true;
        });

        dataTemplateService.deleteByUuid(uuid);
        retry.execute(context -> {
            Assert.assertNull(dataTemplateService.findByUuid(uuid));
            return true;
        });
    }

}
