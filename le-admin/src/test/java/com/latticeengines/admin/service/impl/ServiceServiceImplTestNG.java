package com.latticeengines.admin.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationField;

public class ServiceServiceImplTestNG extends AdminFunctionalTestNGBase {
    
    @Autowired
    private ServiceService serviceService;

    @Test(groups = "functional")
    public void testGetSelectableFields() throws Exception {
        List<SelectableConfigurationField> fields = serviceService.getSelectableConfigurationFields("TestComponent");
        Assert.assertEquals(fields.size(), 1);

        SelectableConfigurationField field = fields.get(0);
        Assert.assertEquals(field.getOptions().size(), 3);
        Assert.assertEquals(field.getDefaultOption(), "1");
    }

    @Test(groups = "functional")
    public void testGetSelectableFieldsFromNonExistingComponent() throws Exception {
        List<SelectableConfigurationField> fields = serviceService.getSelectableConfigurationFields("NotExist");
        Assert.assertTrue(fields.isEmpty());
    }

}
