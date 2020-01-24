package com.latticeengines.modelquality.controller;

import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.modelquality.ModelQualityProxy;
public class PropDataResourceDeploymentTestNG extends ModelQualityDeploymentTestNGBase {

    @Inject
    private ModelQualityProxy modelQualityProxy;

    @Test(groups = "deployment", enabled = true)
    public void createLatestforUI() {
        ObjectMapper objMapper = new ObjectMapper();
        List<PropData> propdatas = objMapper.convertValue(modelQualityProxy.createPropDataConfigFromProductionForUI(),
                new TypeReference<List<PropData>>() {
                });
        Assert.assertEquals(propdatas.size(), 5);

        // cleanup
        for (PropData pd : propdatas) {
            propDataEntityMgr.delete(objMapper.convertValue(pd, PropData.class));
        }
    }
}
