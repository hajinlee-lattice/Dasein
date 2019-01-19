package com.latticeengines.admin.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminDeploymentTestNGBase;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.cdl.CDLComponent;
import com.latticeengines.admin.tenant.batonadapter.datacloud.DataCloudComponent;
import com.latticeengines.admin.tenant.batonadapter.pls.PLSComponent;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantRegistration;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantProperties;

public class TenantResourceDeploymentTestNG extends AdminDeploymentTestNGBase {

    @Autowired
    private TenantService tenantService;

    @Autowired
    private ServiceService serviceService;

    @Test(groups = "deployment", enabled = false)
    public void testCreateTenant() {
        String fullTenantId = "LETest" + System.currentTimeMillis();
        String url = getRestHostPort() + String.format("/admin/tenants/%s/V2?contractId=%s", fullTenantId, fullTenantId);

        TenantProperties tenantProperties = new TenantProperties();
        tenantProperties.description = "A test tenant for new create api";
        tenantProperties.displayName = fullTenantId;
        TenantInfo tenantInfo = new TenantInfo(tenantProperties);

        CustomerSpaceProperties spaceProperties = new CustomerSpaceProperties();
        spaceProperties.description = tenantProperties.description;
        spaceProperties.displayName = tenantProperties.displayName;
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(spaceProperties,
                "{\"Dante\":true}");

        SpaceConfiguration spaceConfiguration = tenantService.getDefaultSpaceConfig();
        spaceConfiguration.setProducts(Arrays.asList(LatticeProduct.LPA3, LatticeProduct.CG));

        // PLS
        SerializableDocumentDirectory plsConfig = serviceService.getDefaultServiceConfig(PLSComponent.componentName);
        for (SerializableDocumentDirectory.Node node : plsConfig.getNodes()) {
            if (node.getNode().contains("SuperAdminEmails")) {
                node.setData("[\"ga_dev@lattice-engines.com\"]");
            } else if (node.getNode().contains("LatticeAdminEmails")) {
                node.setData("[]");
            }
        }
        plsConfig.setRootPath("/" + PLSComponent.componentName);

        // DataCloud
        SerializableDocumentDirectory dataCloudConfig = serviceService
                .getDefaultServiceConfig(DataCloudComponent.componentName);
        dataCloudConfig.setRootPath("/" + DataCloudComponent.componentName);

        SerializableDocumentDirectory cdlConfig = serviceService
                .getDefaultServiceConfig(CDLComponent.componentName);
        cdlConfig.setRootPath("/" + CDLComponent.componentName);

        // Combine configurations
        List<SerializableDocumentDirectory> configDirs = new ArrayList<>();
        configDirs.add(plsConfig);
        configDirs.add(dataCloudConfig);
        configDirs.add(cdlConfig);

        TenantRegistration reg = new TenantRegistration();
        reg.setContractInfo(new ContractInfo(new ContractProperties(fullTenantId, "")));
        reg.setTenantInfo(tenantInfo);
        reg.setSpaceInfo(spaceInfo);
        reg.setSpaceConfig(spaceConfiguration);
        reg.setConfigDirectories(configDirs);

        Boolean result = restTemplate.postForObject(url, reg, Boolean.class);

        Assert.assertNotNull(result);
        Assert.assertTrue(result);
    }
}
