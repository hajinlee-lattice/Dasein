package com.latticeengines.apps.lp.service.impl;


import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.apps.lp.testframework.LPDeploymentTestNGBase;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;

public class LPAttrConfigServiceImplDeploymentTestNG extends LPDeploymentTestNGBase {

    @Inject
    private LPAttrConfigServiceImpl lpAttrConfigService;

    @Test(groups = "deployment")
    void testRenderAndTrim() {
        List<ColumnMetadata> systemMetadata = lpAttrConfigService.getSystemMetadata(BusinessEntity.LatticeAccount);

        List<AttrConfig> customConfig = new ArrayList<>();
        List<AttrConfig> renderConfig = lpAttrConfigService.render(systemMetadata, customConfig);
        List<AttrConfig> copiedList = new ArrayList<AttrConfig>();
        renderConfig.forEach(e -> copiedList.add(e.clone()));

        List<AttrConfig> trimConfig = lpAttrConfigService.trim(renderConfig);
        List<AttrConfig> renderConfig2 = lpAttrConfigService.render(systemMetadata, trimConfig);

        Assert.assertEquals(renderConfig.size(), renderConfig2.size());
        Assert.assertEquals(copiedList, renderConfig2);

        List<AttrConfig> trimConfig2 = lpAttrConfigService.trim(renderConfig2);
        Assert.assertEquals(trimConfig.size(), trimConfig2.size());
        Assert.assertEquals(trimConfig, trimConfig2);
    }
}
