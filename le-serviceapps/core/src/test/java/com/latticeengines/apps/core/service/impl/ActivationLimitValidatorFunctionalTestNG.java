package com.latticeengines.apps.core.service.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.util.StringUtils;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.core.service.ZKConfigService;
import com.latticeengines.apps.core.testframework.ServiceAppsFunctionalTestNGBase;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationDetails.AttrValidation;

public class ActivationLimitValidatorFunctionalTestNG extends ServiceAppsFunctionalTestNGBase {

    private static final int LIMIT = 500;
    private static final int mockHGLimit = 10;

    @InjectMocks
    private ActivationLimitValidator limitationValidator;

    @Mock
    private ZKConfigService zkConfigService;

    @BeforeClass(groups = "functional")
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);
        setupTestEnvironment();
        MultiTenantContext.setTenant(mainTestTenant);
        Mockito.doReturn(mockHGLimit).when(zkConfigService)
                .getMaxPremiumLeadEnrichmentAttributesByLicense(anyString(), anyString());
    }

    @Test(groups = "functional")
    public void testDataLicense() throws Exception {
        List<AttrConfig> attrConfigs = new ArrayList<>();
        AttrConfigProp<AttrState> prop = new AttrConfigProp<>();
        prop.setAllowCustomization(Boolean.TRUE);
        prop.setCustomValue(AttrState.Active);
        for (int i = 0; i < mockHGLimit; i++) {
            AttrConfig config = new AttrConfig();
            config.setAttrName(StringUtils.format("Attr%d", i));
            config.setDataLicense("HG");
            config.putProperty(ColumnMetadataKey.State, prop);
            attrConfigs.add(config);
        }
        limitationValidator.validate(new ArrayList<>(), attrConfigs, new AttrValidation());
        assertEquals(AttrConfigTestUtils.getErrorNumber(attrConfigs), 0);

        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName("Attr_active");
        attrConfig.setDataLicense("HG");
        attrConfig.putProperty(ColumnMetadataKey.State, prop);
        attrConfigs.add(attrConfig);
        limitationValidator.validate(new ArrayList<>(), attrConfigs, new AttrValidation());
        assertEquals(AttrConfigTestUtils.getErrorNumber(attrConfigs), attrConfigs.size());

        attrConfigs.forEach(e -> e.setValidationErrors(null));
        // set state equal to inactive
        AttrConfig inactiveConfig = new AttrConfig();
        inactiveConfig.setAttrName("Attr_inactive");
        inactiveConfig.setDataLicense("HG");
        AttrConfigProp<AttrState> inactiveProp = new AttrConfigProp<>();
        inactiveProp.setAllowCustomization(Boolean.TRUE);
        inactiveProp.setCustomValue(AttrState.Inactive);
        inactiveConfig.putProperty(ColumnMetadataKey.State, inactiveProp);
        attrConfigs.add(inactiveConfig);
        limitationValidator.validate(new ArrayList<>(), attrConfigs, new AttrValidation());
        // the inactive config should not have error message
        assertEquals(AttrConfigTestUtils.getErrorNumber(attrConfigs), attrConfigs.size() - 1);
    }

    @Test(groups = "functional")
    public void testSystemLimit() throws Exception {
        List<AttrConfig> attrConfigs = new ArrayList<>();
        AttrConfigProp<AttrState> stateProp = new AttrConfigProp<>();
        stateProp.setAllowCustomization(Boolean.TRUE);
        stateProp.setCustomValue(AttrState.Active);
        AttrConfigProp<Category> cateProp = new AttrConfigProp<>();
        cateProp.setAllowCustomization(Boolean.TRUE);
        cateProp.setCustomValue(Category.ACCOUNT_ATTRIBUTES);
        for (int i = 0; i < LIMIT; i++) {
            AttrConfig config = new AttrConfig();
            config.setAttrName(StringUtils.format("Attr%d", i));
            config.setEntity(BusinessEntity.Account);
            config.putProperty(ColumnMetadataKey.Category, cateProp);
            config.putProperty(ColumnMetadataKey.State, stateProp);
            attrConfigs.add(config);
        }
        limitationValidator.validate(new ArrayList<>(), attrConfigs, new AttrValidation());
        assertEquals(AttrConfigTestUtils.getErrorNumber(attrConfigs), 0);

        AttrConfig activeConfig = new AttrConfig();
        activeConfig.setAttrName("Attr_active1");
        activeConfig.setEntity(BusinessEntity.Account);
        activeConfig.putProperty(ColumnMetadataKey.Category, cateProp);
        activeConfig.putProperty(ColumnMetadataKey.State, stateProp);
        attrConfigs.add(activeConfig);
        limitationValidator.validate(new ArrayList<>(), attrConfigs, new AttrValidation());
        assertEquals(AttrConfigTestUtils.getErrorNumber(attrConfigs), attrConfigs.size());

        attrConfigs.forEach(e -> e.setValidationErrors(null));
        AttrConfig inactiveConfig = new AttrConfig();
        inactiveConfig.setAttrName("Attr_inactive1");
        inactiveConfig.setEntity(BusinessEntity.Account);
        inactiveConfig.putProperty(ColumnMetadataKey.Category, cateProp);

        AttrConfigProp<AttrState> inactiveProp = new AttrConfigProp<>();
        inactiveProp.setAllowCustomization(Boolean.TRUE);
        inactiveProp.setCustomValue(AttrState.Inactive);
        inactiveConfig.putProperty(ColumnMetadataKey.State, inactiveProp);
        attrConfigs.add(inactiveConfig);
        limitationValidator.validate(new ArrayList<>(), attrConfigs, new AttrValidation());
        // the new config should not have error message
        assertEquals(AttrConfigTestUtils.getErrorNumber(attrConfigs), attrConfigs.size() - 1);
    }

}
