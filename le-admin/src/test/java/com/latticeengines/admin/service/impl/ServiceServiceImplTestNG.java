package com.latticeengines.admin.service.impl;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.admin.functionalframework.TestLatticeComponent;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationDocument;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationField;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.exception.LedpException;

public class ServiceServiceImplTestNG extends AdminFunctionalTestNGBase {
    
    @Autowired
    private ServiceService serviceService;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private TestLatticeComponent testLatticeComponent;

    @BeforeMethod(groups = "functional")
    public void beforeMethod() { testLatticeComponent.doRegistration(); }

    @AfterMethod(groups = "functional")
    public void afterMethod() { testLatticeComponent.doRegistration(); }


    @Test(groups = "functional")
    public void testGetSelectableFields() throws Exception {
        SelectableConfigurationDocument doc = serviceService.getSelectableConfigurationFields("TestComponent", true);
        Assert.assertEquals(doc.getNodes().size(), 1);

        SelectableConfigurationField field = doc.getNodes().get(0);
        Assert.assertEquals(field.getOptions().size(), 3);
        Assert.assertEquals(field.getDefaultOption(), "1");
    }

    @Test(groups = "functional")
    public void testGetSelectableFieldsFromNonExistingComponent() throws Exception {
        SelectableConfigurationDocument doc = serviceService.getSelectableConfigurationFields("NotExist", true);
        Assert.assertNull(doc);
    }

    @Test(groups = "functional", expectedExceptions = LedpException.class)
    public void testPatchOptionsToNonExistingNode() throws Exception {
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
    public void testPatchOptionsToNonExistingService() throws Exception {
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
            dependsOnMethods = {"testPatchOptionsToNonExistingService", "testPatchOptionsToNonExistingNode"})
    public void testPatchOptions() throws Exception {
        SelectableConfigurationDocument doc = new SelectableConfigurationDocument();
        doc.setComponent("TestComponent");
        SelectableConfigurationField field = new SelectableConfigurationField();
        field.setNode("/Config1");
        field.setOptions(Arrays.asList("1", "2"));

        SerializableDocumentDirectory conf = serviceService.getDefaultServiceConfig("TestComponent");
        Assert.assertEquals(conf.getNodeAtPath("/Config1").getMetadata().getOptions().size(), 3);

        Assert.assertTrue(serviceService.patchOptions("TestComponent", field));
        conf = serviceService.getDefaultServiceConfig("TestComponent");

        Assert.assertEquals(conf.getNodeAtPath("/Config1").getMetadata().getOptions().size(), 2);
    }


    @Test(groups = "functional", expectedExceptions = LedpException.class)
    public void testPatchDefaultConfigToNonExistingNode() throws Exception {
        serviceService.patchDefaultConfig("TestComponent", "/nonode", "data");
    }

    @Test(groups = "functional", expectedExceptions = LedpException.class)
    public void testPatchDefaultConfigToInvalidDataType() throws Exception {
        serviceService.patchDefaultConfig("TestComponent", "/Config1", "data");
    }

    @Test(groups = "functional", expectedExceptions = LedpException.class)
    public void testPatchDefaultConfigToOutsideOptions() throws Exception {
        serviceService.patchDefaultConfig("TestComponent", "/Config1", "5");
    }

    @Test(groups = "functional")
    public void testPatchDefaultConfig() throws Exception {
        SerializableDocumentDirectory dir = serviceService.getDefaultServiceConfig("TestComponent");
        SerializableDocumentDirectory.Node node = dir.getNodeAtPath("/Config1");
        Assert.assertEquals(node.getData(), "1");

        serviceService.patchDefaultConfig("TestComponent", "/Config1", "2");

        dir = serviceService.getDefaultServiceConfig("TestComponent");
        node = dir.getNodeAtPath("/Config1");
        Assert.assertEquals(node.getData(), "2");

        serviceService.patchDefaultConfig("TestComponent", "/Config1", "1");

        dir = serviceService.getDefaultServiceConfig("TestComponent");
        node = dir.getNodeAtPath("/Config1");
        Assert.assertEquals(node.getData(), "1");
    }

    @Test(groups = "functional")
    public void testPatchSpaceConfiguration() throws Exception {
        SpaceConfiguration conf = tenantService.getDefaultSpaceConfig();
        Assert.assertEquals(conf.getTopology(), CRMTopology.MARKETO);

        serviceService.patchDefaultConfig(LatticeComponent.spaceConfigNode, "/Topology", CRMTopology.ELOQUA.getName());

        conf = tenantService.getDefaultSpaceConfig();
        Assert.assertEquals(conf.getTopology(), CRMTopology.ELOQUA);

        serviceService.patchDefaultConfig(LatticeComponent.spaceConfigNode, "/Topology", CRMTopology.MARKETO.getName());

        conf = tenantService.getDefaultSpaceConfig();
        Assert.assertEquals(conf.getTopology(), CRMTopology.MARKETO);
    }
}
