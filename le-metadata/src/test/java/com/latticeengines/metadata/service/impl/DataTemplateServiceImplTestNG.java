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
import com.latticeengines.domain.exposed.query.BusinessEntity;
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
        System.out.println("------------------------------------------" + testTenantId);
        DataTemplate dataTemplate = new DataTemplate();
        dataTemplate.setName(DATATEMPLATE_NAME);
        dataTemplate.setEntity(BusinessEntity.Account);
        dataTemplate.setTenant(testTenantId);
        String uuid = dataTemplateService.create(dataTemplate);
        Assert.assertNotNull(uuid);

        Thread.sleep(500);
        DataTemplate foundDataTemplate = dataTemplateService.findByUuid(uuid);
        Assert.assertNotNull(foundDataTemplate);
        Assert.assertEquals(foundDataTemplate.getEntity(), BusinessEntity.Account);

        dataTemplate.setEntity(BusinessEntity.Contact);
        dataTemplateService.updateByUuid(uuid, dataTemplate);
        RetryTemplate retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AssertionError.class), null);
        retry.execute(context -> {
            DataTemplate tempDataTemplate = dataTemplateService.findByUuid(uuid);
            Assert.assertEquals(tempDataTemplate.getEntity(), BusinessEntity.Contact);
            return true;
        });

        dataTemplateService.deleteByUuid(uuid);
        retry.execute(context -> {
            Assert.assertNull(dataTemplateService.findByUuid(uuid));
            return true;
        });
    }

}
