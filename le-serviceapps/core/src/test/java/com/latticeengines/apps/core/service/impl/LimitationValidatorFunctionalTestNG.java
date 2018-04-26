package com.latticeengines.apps.core.service.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.apache.hadoop.util.StringUtils;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.apps.core.testframework.ServiceAppsFunctionalTestNGBase;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.DataLicense;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrSubType;
import com.latticeengines.domain.exposed.serviceapps.core.AttrType;

public class LimitationValidatorFunctionalTestNG extends ServiceAppsFunctionalTestNGBase {

    private static final int LIMIT = 500;
    private static final int mockHGLimit = 10;
    @Mock
    private AttrConfigEntityMgr attrConfigEntityMgr;
    @Spy
    private LimitationValidator limitationValidator;

    @BeforeClass(groups = "functional")
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);
        setupTestEnvironment();
        MultiTenantContext.setTenant(mainTestTenant);
        ReflectionTestUtils.setField(limitationValidator, "attrConfigEntityMgr", attrConfigEntityMgr);
        Mockito.doReturn(mockHGLimit).when(limitationValidator).getMaxPremiumLeadEnrichmentAttributesByLicense(
                anyString(),
                any(DataLicense.class));
    }


    @Test(groups = "functional")
    public void testDataLicense() throws Exception {
        List<AttrConfig> attrConfigs = new ArrayList<>();
        for (int i = 0; i < mockHGLimit; i++) {
            AttrConfig config = new AttrConfig();
            config.setAttrName(StringUtils.format("Attr%d", i));
            config.setDataLicense("HG");
            attrConfigs.add(config);
        }
        limitationValidator.validate(attrConfigs);

        assertEquals(getErrorNumber(attrConfigs), 0);
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName("Attr");
        attrConfig.setDataLicense("HG");
        attrConfigs.add(attrConfig);
        limitationValidator.validate(attrConfigs);
        assertEquals(getErrorNumber(attrConfigs), attrConfigs.size());
    }

    @Test(groups = "functional")
    public void testSystemLimit() throws Exception {
        List<AttrConfig> attrConfigs = new ArrayList<>();
        for (int i = 0; i < LIMIT; i++) {
            AttrConfig config = new AttrConfig();
            config.setAttrName(StringUtils.format("Attr%d", i));
            config.setEntity(BusinessEntity.Account);
            config.setAttrType(AttrType.Custom);
            config.setAttrSubType(AttrSubType.Extension);
            attrConfigs.add(config);
        }
        limitationValidator.validate(attrConfigs);
        assertEquals(getErrorNumber(attrConfigs), 0);

        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName("Attr");
        attrConfig.setEntity(BusinessEntity.Account);
        attrConfig.setAttrType(AttrType.Custom);
        attrConfig.setAttrSubType(AttrSubType.Extension);
        attrConfigs.add(attrConfig);
        limitationValidator.validate(attrConfigs);
        assertEquals(getErrorNumber(attrConfigs), attrConfigs.size());
    }

    private int getErrorNumber(List<AttrConfig> configs) {
        int numErrors = 0;
        for (AttrConfig config : configs) {
            if (config.getValidationErrors() != null) {
                numErrors++;
            }
        }
        return numErrors;
    }
}