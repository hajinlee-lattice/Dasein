package com.latticeengines.admin.service.impl;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationDocument;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationField;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.exception.LedpException;

public class ServiceServiceImplTestNG extends AdminFunctionalTestNGBase {
    
    @Autowired
    private ServiceService serviceService;

    @Test(groups = "functional")
    public void testGetSelectableFields() throws Exception {
        SelectableConfigurationDocument doc = serviceService.getSelectableConfigurationFields("TestComponent");
        Assert.assertEquals(doc.getNodes().size(), 1);

        SelectableConfigurationField field = doc.getNodes().get(0);
        Assert.assertEquals(field.getOptions().size(), 3);
        Assert.assertEquals(field.getDefaultOption(), "1");
    }

    @Test(groups = "functional")
    public void testGetSelectableFieldsFromNonExistingComponent() throws Exception {
        SelectableConfigurationDocument doc = serviceService.getSelectableConfigurationFields("NotExist");
        Assert.assertNull(doc);
    }

    @Test(groups = "functional", expectedExceptions = LedpException.class)
    public void testPathOptionsToNonExistingNode() throws Exception {
        SelectableConfigurationDocument doc = new SelectableConfigurationDocument();
        doc.setComponent("TestComponent");
        SelectableConfigurationField field = new SelectableConfigurationField();
        field.setNode("/nonode");
        field.setOptions(Arrays.asList("1","2"));

        SerializableDocumentDirectory conf = serviceService.getDefaultServiceConfig("TestComponent");
        Assert.assertEquals(conf.getNodeAtPath("/Config1").getMetadata().getOptions().size(), 3);

        serviceService.patchOptions("NoComponent", field);
    }

    @Test(groups = "functional", expectedExceptions = LedpException.class)
    public void testPathOptionsToNonExistingService() throws Exception {
        SelectableConfigurationDocument doc = new SelectableConfigurationDocument();
        doc.setComponent("NoComponent");
        SelectableConfigurationField field = new SelectableConfigurationField();
        field.setNode("/Config1");
        field.setOptions(Arrays.asList("1","2"));

        SerializableDocumentDirectory conf = serviceService.getDefaultServiceConfig("TestComponent");
        Assert.assertEquals(conf.getNodeAtPath("/Config1").getMetadata().getOptions().size(), 3);

        serviceService.patchOptions("NoComponent", field);
    }

    @Test(groups = "functional",
            dependsOnMethods = {"testPathOptionsToNonExistingService", "testPathOptionsToNonExistingNode"})
    public void testPathOptions() throws Exception {
        SelectableConfigurationDocument doc = new SelectableConfigurationDocument();
        doc.setComponent("TestComponent");
        SelectableConfigurationField field = new SelectableConfigurationField();
        field.setNode("/Config1");
        field.setOptions(Arrays.asList("1","2"));

        SerializableDocumentDirectory conf = serviceService.getDefaultServiceConfig("TestComponent");
        Assert.assertEquals(conf.getNodeAtPath("/Config1").getMetadata().getOptions().size(), 3);

        Assert.assertTrue(serviceService.patchOptions("TestComponent", field));
        conf = serviceService.getDefaultServiceConfig("TestComponent");

        Assert.assertEquals(conf.getNodeAtPath("/Config1").getMetadata().getOptions().size(), 2);
    }

}
