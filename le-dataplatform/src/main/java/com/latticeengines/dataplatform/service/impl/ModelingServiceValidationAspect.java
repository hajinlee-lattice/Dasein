package com.latticeengines.dataplatform.service.impl;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;

@Aspect
public class ModelingServiceValidationAspect {

    @Before("execution(* com.latticeengines.dataplatform.exposed.service.impl.ModelingServiceImpl.loadData(..)) "
            + " && args(config)")
    public void validateLoad(LoadConfiguration config) {
        validateLoadConfig(config);
    }

    @Before("execution(* com.latticeengines.dataplatform.exposed.service.impl.ModelingServiceImpl.createSamples(..)) "
            + " && args(config)")
    public void validateCreateSamples(SamplingConfiguration config) {
        validateCreateSamplesConfig(config);
    }

    @Before("execution(* com.latticeengines.dataplatform.exposed.service.impl.ModelingServiceImpl.profileData(..)) "
            + " && args(config)")
    public void validateProfileData(DataProfileConfiguration config) {
        validateProfileDataConfig(config);
    }
    
    @Before("execution(* com.latticeengines.dataplatform.exposed.service.impl.ModelingServiceImpl.submitModel(..)) "
            + " && args(model)")
    public void validateSubmitMode(Model model) {
        validateSubmitModelConfig(model);
    }

    void validateLoadConfig(LoadConfiguration config) {
        if (!isElementValidInPath(config.getCustomer())) {
            throw new LedpException(LedpCode.LEDP_10007, new String[] { config.getCustomer() });
        }
        if (!isElementValidInPath(config.getTable())) {
            throw new LedpException(LedpCode.LEDP_10007, new String[] { config.getTable() });
        }
        if (!isElementValidInPath(config.getMetadataTable())) {
            throw new LedpException(LedpCode.LEDP_10007, new String[] { config.getMetadataTable() });
        }
    }
    
    void validateCreateSamplesConfig(SamplingConfiguration config) {
        if (!isElementValidInPath(config.getCustomer())) {
            throw new LedpException(LedpCode.LEDP_10007, new String[] { config.getCustomer() });
        }
        if (!isElementValidInPath(config.getTable())) {
            throw new LedpException(LedpCode.LEDP_10007, new String[] { config.getTable() });
        }
    }
    
    void validateProfileDataConfig(DataProfileConfiguration config) {
        if (!isElementValidInPath(config.getCustomer())) {
            throw new LedpException(LedpCode.LEDP_10007, new String[] { config.getCustomer() });
        }
        if (!isElementValidInPath(config.getTable())) {
            throw new LedpException(LedpCode.LEDP_10007, new String[] { config.getTable() });
        }
    }
    
    void validateSubmitModelConfig(Model config) {
        if (!isElementValidInPath(config.getCustomer())) {
            throw new LedpException(LedpCode.LEDP_10007, new String[] { config.getCustomer() });
        }
        if (!isElementValidInPath(config.getTable())) {
            throw new LedpException(LedpCode.LEDP_10007, new String[] { config.getTable() });
        }
    }
    
    private boolean isElementValidInPath(String value) {

        if (StringUtils.containsAny(value, "{}[]/:")) {
            return false;
        }
        String[] invalidList = { "", ".", "..", "/" };
        Set<String> invalidElements = new HashSet<>(Arrays.asList(invalidList));
        if (invalidElements.contains(value)) {
            return false;
        }

        return true;
    }
}
