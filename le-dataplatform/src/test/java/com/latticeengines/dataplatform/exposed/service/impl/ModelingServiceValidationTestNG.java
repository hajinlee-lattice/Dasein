package com.latticeengines.dataplatform.exposed.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;

public class ModelingServiceValidationTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private ModelingService modelingService;

    
    @Test(groups = "functional")
    public void validateLoad() {
        LoadConfiguration config = new LoadConfiguration();
        config.setCustomer("{Dell}");

        LedpException ex = null;
        try {
            modelingService.loadData(config);
        } catch (LedpException ex2) {
            ex = ex2;
        }
        Assert.assertTrue(ex instanceof LedpException);
        Assert.assertEquals(ex.getCode(), LedpCode.LEDP_10007);
    }
    
    @Test(groups = "functional")
    public void validateCreateSamples() {
        SamplingConfiguration config = new SamplingConfiguration();
        config.setCustomer("{Dell}");
        LedpException ex = null;
        try {
            modelingService.createSamples(config);
        } catch (LedpException ex2) {
            ex = ex2;
        }
        Assert.assertTrue(ex instanceof LedpException);
        Assert.assertEquals(ex.getCode(), LedpCode.LEDP_10007);
    }
    
    @Test(groups = "functional")
    public void validateProfileData() {
        DataProfileConfiguration config = new DataProfileConfiguration();
        config.setTable("{Dell}");
        LedpException ex = null;
        try {
            modelingService.profileData(config);
        } catch (LedpException ex2) {
            ex = ex2;
        }
        Assert.assertTrue(ex instanceof LedpException);
        Assert.assertEquals(ex.getCode(), LedpCode.LEDP_10007);
    }
    
    @Test(groups = "functional")
    public void validateSubmitModel() {
        Model model = new Model();
        model.setCustomer("{Dell}");
        LedpException ex = null;
        try {
            modelingService.submitModel(model);
        } catch (LedpException ex2) {
            ex = ex2;
        }
        Assert.assertTrue(ex instanceof LedpException);
        Assert.assertEquals(ex.getCode(), LedpCode.LEDP_10007);
    }
}
