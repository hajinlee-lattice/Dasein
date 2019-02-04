package com.latticeengines.pls.controller;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.commons.collections4.CollectionUtils;
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

        List<String> playTypeNames = playTypeList.stream().map(PlayType::getDisplayName).collect(Collectors.toList());
        List<String> defaultTypes = Arrays.asList("List", "Cross-Sell", "Prospecting", "Renewal", "Upsell");
        defaultTypes.forEach(e -> Assert.assertTrue(playTypeNames.contains(e)));
    }

    @Test(groups = "deployment", dependsOnMethods = "testGetAllTypes")
    public void testCreate() {
        // Test creating a new playType
        playType = new PlayType(tenant, playTypeName, "Description", "admin.le.com", "admin.le.com");
        playType.setId(null);
        playType = playProxy.createPlayType(tenant.getId(), playType);
        Assert.assertNotNull(playType.getId());
        Assert.assertNotEquals(playType.getPid(), 0);
        List<PlayType> types = playProxy.getPlayTypes(tenant.getId()).stream()
                .filter(pl -> pl.getDisplayName().equals(playTypeName)).collect(Collectors.toList());
        Assert.assertNotNull(types);
        Assert.assertEquals(types.size(), 1);
        playType = types.get(0);
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreate")
    public void testGetById() {
        sleepToAllowDbWriterReaderSync();
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
        playProxy.updatePlayType(tenant.getId(), playType.getId(), playType);
        sleepToAllowDbWriterReaderSync();
        PlayType updatedPlayType = playProxy.getPlayTypeById(tenant.getId(), playType.getId());
        Assert.assertNotNull(updatedPlayType);
        Assert.assertEquals(updatedPlayType.getDisplayName(), updatedPlayTypeName);
    }

    @Test(groups = "deployment", dependsOnMethods = "testUpdate")
    public void testDelete() {
        // Test deleting playType
        playProxy.deletePlayTypeById(tenant.getId(), playType.getId());
        List<PlayType> playTypeList = playProxy.getPlayTypes(tenant.getId());
        Assert.assertNotNull(playTypeList);
        List<PlayType> playTypes =
                playTypeList.stream().filter(pt -> pt.getId().equals(playType.getId())).collect(Collectors.toList());
        Assert.assertTrue(CollectionUtils.isEmpty(playTypes));
    }

    private void sleepToAllowDbWriterReaderSync() {
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            // Ignore
        }
    }
}
