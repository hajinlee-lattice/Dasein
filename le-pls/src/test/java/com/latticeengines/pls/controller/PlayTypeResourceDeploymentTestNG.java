package com.latticeengines.pls.controller;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.pls.PlayType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;

@Component
public class PlayTypeResourceDeploymentTestNG extends PlsDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(PlayTypeResourceDeploymentTestNG.class);

    @Inject
    private PlayProxy playProxy;

    private Tenant tenant;
    private PlayType playType;
    private String playTypeName = "playTypeTest";

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        tenant = testBed.getMainTestTenant();
    }

    @Test(groups = "deployment")
    public void testGetAllTypes() {
        // Test that creating a new tenant will always have these 5 default
        // playTypes
        List<PlayType> playTypeList = playProxy.getPlayTypes(tenant.getId());
        Assert.assertNotNull(playTypeList);
        Assert.assertEquals(playTypeList.size(), 5);

        List<String> playTypeNames = playTypeList.stream().map(e -> e.getDisplayName()).collect(Collectors.toList());
        List<String> defaultTypes = Arrays.asList("List", "Cross-Sell", "Prospecting", "Renewal", "Upsell");

        defaultTypes.stream().forEach(e -> {
            Assert.assertTrue(playTypeNames.contains(e));
        });
    }

    @Test(groups = "deployment", dependsOnMethods = "testGetAllTypes")
    public void testCreate() {
        // Test creating a new playType
        playType = new PlayType(tenant, playTypeName, "Description", "admin.le.com", "admin.le.com");
        playType.setId(null);
        playType = playProxy.createPlayType(tenant.getId(), playType);
        Assert.assertNotNull(playType);
        Assert.assertEquals(playTypeName, playType.getDisplayName());
        List<PlayType> playTypeList = playProxy.getPlayTypes(tenant.getId());
        Assert.assertNotNull(playTypeList);
        List<String> playTypeNames = playTypeList.stream().map(e -> e.getDisplayName()).collect(Collectors.toList());
        Assert.assertTrue(playTypeNames.contains(playType.getDisplayName()));
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreate")
    public void testGetById() {
        // Test getting the newly made playType by Id
        String playTypeId = playType.getId();
        PlayType getPlayType = playProxy.getPlayTypeById(tenant.getId(), playTypeId);
        Assert.assertNotNull(getPlayType);
        Assert.assertEquals(getPlayType.getDisplayName(), playType.getDisplayName());
    }

    @Test(groups = "deployment", dependsOnMethods = "testGetById")
    public void testUpdate() {
        // Test updating the newly made playType
        String updatedPlayTypeName = "playTypeTestPostUpdate";
        playType.setDisplayName(updatedPlayTypeName);
        PlayType updatedPlayType = playProxy.updatePlayType(tenant.getId(), playType.getId(), playType);
        Assert.assertNotNull(updatedPlayType);
        Assert.assertEquals(updatedPlayType.getDisplayName(), updatedPlayTypeName);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = "deployment", dependsOnMethods = "testUpdate")
    public void testDelete() {
        // Test deleting playType
        playProxy.deletePlayTypeById(tenant.getId(), playType.getId());
        List<PlayType> playTypeList = playProxy.getPlayTypes(tenant.getId());
        Assert.assertNotNull(playTypeList);
        List<String> playTypeNames = playTypeList.stream().map(e -> e.getDisplayName()).collect(Collectors.toList());
        Assert.assertFalse(playTypeNames.contains(playType.getDisplayName()));
    }
}
