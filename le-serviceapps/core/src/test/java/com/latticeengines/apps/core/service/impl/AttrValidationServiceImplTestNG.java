package com.latticeengines.apps.core.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.latticeengines.apps.core.service.AttrValidationService;
import com.latticeengines.apps.core.testframework.ServiceAppsFunctionalTestNGBase;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceapps.core.AttrType;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationDetails;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationErrors;

public class AttrValidationServiceImplTestNG extends ServiceAppsFunctionalTestNGBase {

    @Inject
    private AttrValidationService attrValidationService;

    private List<AttrConfig> attrConfigList = new ArrayList<>();

    @BeforeTest
    public void setup() {
        attrConfigList.addAll(Arrays.asList(getAccountId(),getAnnualRevenue(), getCustomeAccountAttr(),
                getContactId(), getContactFirstName()));
    }

    private AttrConfig getAccountId() {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName(InterfaceName.AccountId.name());
        attrConfig.setAttrType(AttrType.Internal);
        attrConfig.setEntity(BusinessEntity.Account);
        AttrConfigProp<String> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(true);
        prop1.setSystemValue("AccountId");
        prop1.setCustomValue("ID");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop1);

        AttrConfigProp<String> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(true);
        prop2.setSystemValue("Category1");
        prop2.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.Category, prop2);

        AttrConfigProp<String> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(true);
        prop3.setSystemValue("Account ID");
        prop3.setCustomValue("Internal Account ID");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop3);

        AttrConfigProp<AttrState> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(true);
        prop4.setSystemValue(AttrState.Active);
        prop4.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.State, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(false);
        prop5.setSystemValue("NONE");
        prop5.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.ApprovedUsage, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(true);
        prop6.setSystemValue(true);
        prop6.setCustomValue(null);
        attrConfig.putProperty(ColumnSelection.Predefined.TalkingPoint.name(), prop6);
        return attrConfig;
    }

    private AttrConfig getAnnualRevenue() {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName(InterfaceName.AnnualRevenue.name());
        attrConfig.setAttrType(AttrType.Internal);
        attrConfig.setEntity(BusinessEntity.Account);
        AttrConfigProp<String> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(true);
        prop1.setSystemValue("AnnualRevenue");
        prop1.setCustomValue("Annual_Revenue");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop1);

        AttrConfigProp<String> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(true);
        prop2.setSystemValue("Category2");
        prop2.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.Category, prop2);

        AttrConfigProp<String> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(true);
        prop3.setSystemValue(null);
        prop3.setCustomValue("AnnualRevenue");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop3);

        AttrConfigProp<AttrState> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(true);
        prop4.setSystemValue(AttrState.Inactive);
        prop4.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.State, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(false);
        prop5.setSystemValue("ModelAndAllInsigts");
        prop5.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.ApprovedUsage, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(true);
        prop6.setSystemValue(true);
        prop6.setCustomValue(null);
        attrConfig.putProperty(ColumnSelection.Predefined.TalkingPoint.name(), prop6);
        return attrConfig;
    }

    private AttrConfig getCustomeAccountAttr() {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName("Email");
        attrConfig.setAttrType(AttrType.Custom);
        attrConfig.setEntity(BusinessEntity.Account);
        AttrConfigProp<String> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(true);
        prop1.setSystemValue("Email");
        prop1.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop1);

        AttrConfigProp<String> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(true);
        prop2.setSystemValue("Category2");
        prop2.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.Category, prop2);

        AttrConfigProp<String> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(true);
        prop3.setSystemValue("Email");
        prop3.setCustomValue("Email Address");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop3);

        AttrConfigProp<AttrState> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(true);
        prop4.setSystemValue(AttrState.Active);
        prop4.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.State, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(false);
        prop5.setSystemValue("None");
        prop5.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.ApprovedUsage, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(true);
        prop6.setSystemValue(true);
        prop6.setCustomValue(null);
        attrConfig.putProperty(ColumnSelection.Predefined.TalkingPoint.name(), prop6);
        return attrConfig;
    }

    private AttrConfig getContactId() {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName(InterfaceName.ContactId.name());
        attrConfig.setAttrType(AttrType.Internal);
        attrConfig.setEntity(BusinessEntity.Contact);
        AttrConfigProp<String> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(true);
        prop1.setSystemValue("ContactId");
        prop1.setCustomValue("Contact_Id");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop1);

        AttrConfigProp<String> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(true);
        prop2.setSystemValue("Category1");
        prop2.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.Category, prop2);

        AttrConfigProp<String> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(true);
        prop3.setSystemValue("ContactID");
        prop3.setCustomValue("Contact ID");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop3);

        AttrConfigProp<AttrState> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(true);
        prop4.setSystemValue(AttrState.Active);
        prop4.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.State, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(false);
        prop5.setSystemValue("None");
        prop5.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.ApprovedUsage, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(true);
        prop6.setSystemValue(true);
        prop6.setCustomValue(null);
        attrConfig.putProperty(ColumnSelection.Predefined.Segment.name(), prop6);
        return attrConfig;
    }

    private AttrConfig getContactFirstName() {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setAttrName(InterfaceName.FirstName.name());
        attrConfig.setAttrType(AttrType.Internal);
        attrConfig.setEntity(BusinessEntity.Contact);
        AttrConfigProp<String> prop1 = new AttrConfigProp<>();
        prop1.setAllowCustomization(true);
        prop1.setSystemValue("FirstName");
        prop1.setCustomValue("First Name");
        attrConfig.putProperty(ColumnMetadataKey.DisplayName, prop1);

        AttrConfigProp<String> prop2 = new AttrConfigProp<>();
        prop2.setAllowCustomization(true);
        prop2.setSystemValue("Category1");
        prop2.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.Category, prop2);

        AttrConfigProp<String> prop3 = new AttrConfigProp<>();
        prop3.setAllowCustomization(true);
        prop3.setSystemValue(null);
        prop3.setCustomValue("First Name");
        attrConfig.putProperty(ColumnMetadataKey.Description, prop3);

        AttrConfigProp<AttrState> prop4 = new AttrConfigProp<>();
        prop4.setAllowCustomization(true);
        prop4.setSystemValue(AttrState.Active);
        prop4.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.State, prop4);

        AttrConfigProp<String> prop5 = new AttrConfigProp<>();
        prop5.setAllowCustomization(false);
        prop5.setSystemValue("None");
        prop5.setCustomValue(null);
        attrConfig.putProperty(ColumnMetadataKey.ApprovedUsage, prop5);

        AttrConfigProp<Boolean> prop6 = new AttrConfigProp<>();
        prop6.setAllowCustomization(true);
        prop6.setSystemValue(true);
        prop6.setCustomValue(null);
        attrConfig.putProperty(ColumnSelection.Predefined.Segment.name(), prop6);
        return attrConfig;
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
        attrConfigs.add(attrConfig);
        ValidationDetails details = attrValidationService.validate(attrConfigs);
        Assert.assertNotNull(details);
        Assert.assertEquals(details.getValidations().size(), 1);
        ValidationDetails.AttrValidation validation = details.getValidations().get(0);
        Assert.assertNotNull(validation.getValidationErrors());
        Assert.assertNull(validation.getImpactWarnings());
        Assert.assertEquals(validation.getAttrName(), "TestAttr");
        Assert.assertTrue(validation.getValidationErrors().getErrors().containsKey(ValidationErrors.Type.INVALID_PROP_CHANGE));
    }


}
