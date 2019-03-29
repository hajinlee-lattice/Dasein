package com.latticeengines.apps.core.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.core.service.AttrValidationService;
import com.latticeengines.apps.core.testframework.ServiceAppsFunctionalTestNGBase;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigUpdateMode;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationDetails;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationErrors;

public class AttrValidationServiceImplTestNG extends ServiceAppsFunctionalTestNGBase {

    @Inject
    private AttrValidationService attrValidationService;

    private List<AttrConfig> attrConfigList = new ArrayList<>();

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
        attrConfigList.addAll(Arrays.asList(AttrConfigTestUtils.getAccountId(), AttrConfigTestUtils.getAnnualRevenue(),
                AttrConfigTestUtils.getCustomeAccountAttr(), AttrConfigTestUtils.getContactId(),
                AttrConfigTestUtils.getContactFirstName()));
    }

    @Test(groups = "functional")
    public void testInvalidPropChange() {
        List<AttrConfig> attrConfigs = new ArrayList<>();
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName("TestAttr");
        AttrConfigProp<Integer> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(false);
        prop1.setSystemValue(100);
        prop1.setCustomValue(99);
        attrConfig.putProperty("IntValue", prop1);
        AttrConfigProp<String> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(false);
        prop2.setSystemValue("TestAttr");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop2);
        attrConfigs.add(attrConfig);
        ValidationDetails details = attrValidationService.validate(attrConfigList, attrConfigs,
                AttrConfigUpdateMode.Usage);
        Assert.assertNotNull(details);
        Assert.assertEquals(details.getValidations().size(), 1);
        ValidationDetails.AttrValidation validation = details.getValidations().get(0);
        Assert.assertNotNull(validation.getValidationErrors());
        Assert.assertNull(validation.getImpactWarnings());
        Assert.assertEquals(validation.getAttrName(), "TestAttr");
        Assert.assertTrue(
                validation.getValidationErrors().getErrors().containsKey(ValidationErrors.Type.INVALID_PROP_CHANGE));
    }

}
