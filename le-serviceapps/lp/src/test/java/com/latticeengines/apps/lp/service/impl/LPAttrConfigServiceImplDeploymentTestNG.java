package com.latticeengines.apps.lp.service.impl;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.apps.lp.testframework.LPDeploymentTestNGBase;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;

/**
 * $ dpltc deploy -a admin,matchapi,pls,metadata,cdl,lp
 */
public class LPAttrConfigServiceImplDeploymentTestNG extends LPDeploymentTestNGBase {

    @Inject
    private LPAttrConfigServiceImpl lpAttrConfigService;

    @Test(groups = "deployment", dataProvider = "FeatureFlags")
    void testRenderAndTrim(boolean entityMatchEnabled) {
        List<ColumnMetadata> systemMetadata = lpAttrConfigService.getSystemMetadata(BusinessEntity.Account);

        List<AttrConfig> customConfig = new ArrayList<>();
        List<AttrConfig> renderConfig = lpAttrConfigService.render(systemMetadata, customConfig, entityMatchEnabled);
        List<AttrConfig> copiedList = new ArrayList<>();
        renderConfig.forEach(e -> copiedList.add(e.clone()));

        List<AttrConfig> trimConfig = lpAttrConfigService.trim(renderConfig);
        List<AttrConfig> renderConfig2 = lpAttrConfigService.render(systemMetadata, trimConfig, entityMatchEnabled);

        Assert.assertEquals(renderConfig.size(), renderConfig2.size());
        Assert.assertEquals(copiedList, renderConfig2);

        List<AttrConfig> trimConfig2 = lpAttrConfigService.trim(renderConfig2);
        Assert.assertEquals(trimConfig.size(), trimConfig2.size());
        Assert.assertEquals(trimConfig, trimConfig2);
    }

    // Schema: EntityMatchEnabled
    @DataProvider(name = "FeatureFlags")
    private Object[][] getFeatureFlags() {
        return new Object[][] {
                { false }, //
                { true }
        };
    }
}
