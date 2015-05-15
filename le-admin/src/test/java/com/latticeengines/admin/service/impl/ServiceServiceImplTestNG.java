package com.latticeengines.admin.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationDocument;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationField;

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
//        Assert.assertTrue(doc.getNodes().isEmpty());
    }

}
