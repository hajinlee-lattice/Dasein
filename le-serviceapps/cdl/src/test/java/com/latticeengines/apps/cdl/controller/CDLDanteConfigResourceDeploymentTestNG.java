package com.latticeengines.apps.cdl.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.dante.DanteConfigurationDocument;
import com.latticeengines.domain.exposed.dante.metadata.NotionMetadataWrapper;
import com.latticeengines.domain.exposed.dante.metadata.PropertyMetadata;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigUpdateMode;
import com.latticeengines.domain.exposed.util.ApsGeneratorUtils;
import com.latticeengines.domain.exposed.util.CategoryUtils;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;
import com.latticeengines.proxy.exposed.cdl.CDLDanteConfigProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;

public class CDLDanteConfigResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CDLDanteConfigResourceDeploymentTestNG.class);

    @Inject
    private CDLTestDataService cdlTestDataService;

    @Inject
    private CDLDanteConfigProxy cdlDanteConfigProxy;

    @Inject
    private CDLAttrConfigProxy cdlAttrConfigProxy;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    private DanteConfigurationDocument danteConfig;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironment();
        cdlTestDataService.populateMetadata(mainCustomerSpace, 5);
    }

    @Test(groups = "deployment")
    public void testGetDanteConfig() {
        danteConfig = cdlDanteConfigProxy.getDanteConfiguration(MultiTenantContext.getShortTenantId());
        Assert.assertNotNull(danteConfig);
    }

    @Test(groups = "deployment")
    public void testRefreshDanteConfig() throws InterruptedException {
        List<ColumnMetadata> columnMetadata = servingStoreProxy.getAccountMetadata(mainCustomerSpace,
                ColumnSelection.Predefined.TalkingPoint, null);
        ColumnMetadata attrToTest = columnMetadata.get(new Random().nextInt(columnMetadata.size() + 100) - 100);
        Assert.assertNotNull(attrToTest);
        log.info("TestAttr: " + attrToTest.getAttrName());
        danteConfig = cdlDanteConfigProxy.getDanteConfiguration(MultiTenantContext.getShortTenantId());
        Assert.assertNotNull(danteConfig);

        PropertyMetadata pm = danteConfig.getMetadataDocument().getNotions().stream()
                .filter(notion -> notion.getKey().equals("DanteAccount")).findFirst()
                .orElse(new NotionMetadataWrapper()).getValue().getProperties().stream()
                .filter(x -> x.getName().equals(attrToTest.getAttrName())).findFirst().orElse(null);
        Assert.assertNotNull(pm);

        AttrConfigRequest attrConfigRequest = generateAttrConfigRequest(attrToTest);
        cdlAttrConfigProxy.saveAttrConfig(mainCustomerSpace, attrConfigRequest, AttrConfigUpdateMode.Usage, true);
        Thread.sleep(120000);
        danteConfig = cdlDanteConfigProxy.getDanteConfiguration(MultiTenantContext.getShortTenantId());
        Assert.assertNotNull(danteConfig);
        Assert.assertFalse(danteConfig.getMetadataDocument().getNotions().stream()
                .filter(notion -> notion.getKey().equals("DanteAccount")).findFirst()
                .orElse(new NotionMetadataWrapper()).getValue().getProperties().stream()
                .anyMatch(x -> x.getName().equals(attrToTest.getAttrName())));

    }

    private AttrConfigRequest generateAttrConfigRequest(ColumnMetadata attrToTest) {
        AttrConfigRequest attrConfigRequest = new AttrConfigRequest();
        List<AttrConfig> attrConfigs = new ArrayList<>();
        attrConfigRequest.setAttrConfigs(attrConfigs);
        AttrConfig config = new AttrConfig();
        config.setAttrName(attrToTest.getAttrName());
        if (Category.PRODUCT_SPEND.equals(attrToTest.getCategory())) {
            if (ApsGeneratorUtils.isApsAttr(attrToTest.getAttrName())) {
                config.setEntity(BusinessEntity.AnalyticPurchaseState);
            } else {
                config.setEntity(BusinessEntity.PurchaseHistory);
            }
        } else {
            config.setEntity(CategoryUtils.getEntity(attrToTest.getCategory()).get(0));
        }
        AttrConfigProp<Boolean> enrichProp = new AttrConfigProp<>();
        enrichProp.setCustomValue(Boolean.FALSE);
        config.setAttrProps(ImmutableMap.of("TalkingPoint", enrichProp));
        attrConfigs.add(config);
        return attrConfigRequest;
    }
}
